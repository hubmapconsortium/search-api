import concurrent.futures
import copy
import importlib
import json
import logging
import os
import re
import sys
import time
from yaml import safe_load

# For reusing the app.cfg configuration when running indexer_base.py as script
from flask import Flask, Response

# HuBMAP commons
from hubmap_commons import globus_groups
from hubmap_commons.hm_auth import AuthHelper

sys.path.append("search-adaptor/src")
from indexer import Indexer
from opensearch_helper_functions import *
from translator.tranlation_helper_functions import *
from translator.translator_interface import TranslatorInterface

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# This list contains fields that are added to the top-level at index runtime
entity_properties_list = [
    'metadata',
    'donor',
    'origin_sample',
    'source_sample',
    'ancestor_ids',
    'descendant_ids',
    'ancestors',
    'descendants',
    'files',
    'immediate_ancestors',
    'immediate_descendants',
    'datasets'
]

# Entity types that will have `display_subtype` generated ar index time
entity_types_with_display_subtype = ['Upload', 'Donor', 'Sample', 'Dataset']


class Translator(TranslatorInterface):
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    DATASET_STATUS_PUBLISHED = 'published'
    DEFAULT_INDEX_WITHOUT_PREFIX = ''
    INDICES = {}
    TRANSFORMERS = {}
    DEFAULT_ENTITY_API_URL = ''
    indexer = None
    entity_api_cache = {}

    def __init__(self, indices, app_client_id, app_client_secret, token):
        try:
            self.indices: dict = {}
            # Do not include the indexes that are self managed...
            for key, value in indices['indices'].items():
                if 'reindex_enabled' in value and value['reindex_enabled'] is True:
                    self.indices[key] = value
            self.DEFAULT_INDEX_WITHOUT_PREFIX: str = indices['default_index']
            self.INDICES: dict = {'default_index': self.DEFAULT_INDEX_WITHOUT_PREFIX, 'indices': self.indices}
            self.DEFAULT_ENTITY_API_URL = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX][
                'document_source_endpoint'].strip(
                '/')

            self.indexer = Indexer(self.indices, self.DEFAULT_INDEX_WITHOUT_PREFIX)

            logger.debug("@@@@@@@@@@@@@@@@@@@@ INDICES")
            logger.debug(self.INDICES)
        except Exception:
            raise ValueError("Invalid indices config")

        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token

        auth_helper = self.init_auth_helper()
        self.request_headers = self.create_request_headers_for_auth(token)

        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')

        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent / 'VERSION').read_text()).strip()

        with open(Path(__file__).resolve().parent / 'hubmap_translation' / 'neo4j-to-es-attributes.json',
                  'r') as json_file:
            self.attr_map = json.load(json_file)

        # # Preload all the transformers
        self.init_transformers()

    def translate_all(self):
        with app.app_context():
            try:
                logger.info("############# translate_all() Started #############")

                start = time.time()

                # Make calls to entity-api to get a list of uuids for each entity type
                donor_uuids_list = get_uuids_by_entity_type("donor", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                sample_uuids_list = get_uuids_by_entity_type("sample", self.request_headers,
                                                             self.DEFAULT_ENTITY_API_URL)
                dataset_uuids_list = get_uuids_by_entity_type("dataset", self.request_headers,
                                                              self.DEFAULT_ENTITY_API_URL)
                upload_uuids_list = get_uuids_by_entity_type("upload", self.request_headers,
                                                             self.DEFAULT_ENTITY_API_URL)
                public_collection_uuids_list = get_uuids_by_entity_type("collection", self.request_headers,
                                                                        self.DEFAULT_ENTITY_API_URL)

                logger.debug("merging sets into a one list...")
                # Merge into a big list that with no duplicates
                all_entities_uuids = set(
                    donor_uuids_list + sample_uuids_list + dataset_uuids_list + upload_uuids_list + public_collection_uuids_list)

                es_uuids = []
                # for index in ast.literal_eval(app.config['INDICES']).keys():
                logger.debug("looping through the indices...")
                logger.debug(self.INDICES['indices'].keys())

                index_names = get_all_reindex_enabled_indice_names(self.INDICES)
                logger.debug(self.INDICES['indices'].keys())

                for index in index_names.keys():
                    all_indices = index_names[index]
                    # get URL for that index
                    es_url = self.INDICES['indices'][index]['elasticsearch']['url'].strip('/')

                    for actual_index in all_indices:
                        es_uuids.extend(get_uuids_from_es(actual_index, es_url))

                es_uuids = set(es_uuids)

                logger.debug("looping through the UUIDs...")

                # Remove entities found in Elasticsearch but no longer in neo4j
                for uuid in es_uuids:
                    if uuid not in all_entities_uuids:
                        logger.debug(
                            f"Entity of uuid: {uuid} found in Elasticsearch but no longer in neo4j. Delete it from Elasticsearch.")
                        self.delete(uuid)

                logger.debug("Starting multi-thread reindexing ...")

                # Reindex in multi-treading mode for:
                # - each public collection
                # - each upload, only add to the hm_consortium_entities index (private index of the default)
                # - each donor and its descendants in the tree
                futures_list = []
                results = []
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    public_collection_futures_list = [
                        executor.submit(self.translate_public_collection, uuid, reindex=True)
                        for uuid in public_collection_uuids_list]
                    upload_futures_list = [executor.submit(self.translate_upload, uuid, reindex=True) for uuid in
                                           upload_uuids_list]
                    donor_futures_list = [executor.submit(self.translate_tree, uuid) for uuid in donor_uuids_list]

                    # Append the above three lists into one
                    futures_list = public_collection_futures_list + upload_futures_list + donor_futures_list

                    for f in concurrent.futures.as_completed(futures_list):
                        logger.debug(f.result())

                end = time.time()

                logger.info(
                    f"############# translate_all() Completed. Total time used: {end - start} seconds. #############")
            except Exception as e:
                logger.error(e)

    def translate(self, entity_id):
        try:
            # Retrieve the entity details
            entity = self.call_entity_api(entity_id, 'entities')

            # Check if entity is empty
            if bool(entity):
                logger.info(f"Executing translate() for entity_id: {entity_id}, entity_type: {entity['entity_type']}")

                if entity['entity_type'] == 'Collection':
                    self.translate_public_collection(entity_id, reindex=True)
                elif entity['entity_type'] == 'Upload':
                    self.translate_upload(entity_id, reindex=True)
                else:
                    previous_revision_entity_ids = []
                    next_revision_entity_ids = []

                    ancestor_entity_ids = self.call_entity_api(entity_id, 'ancestors', 'uuid')
                    descendant_entity_ids = self.call_entity_api(entity_id, 'descendants', 'uuid')

                    # Only Dataset entities may have previous/next revisions
                    if entity['entity_type'] == 'Dataset':
                        previous_revision_entity_ids = self.call_entity_api(entity_id, 'previous_revisions',
                                                                            'uuid')
                        next_revision_entity_ids = self.call_entity_api(entity_id, 'next_revisions', 'uuid')

                    # All entity_ids in the path excluding the entity itself
                    entity_ids = ancestor_entity_ids + descendant_entity_ids + previous_revision_entity_ids + next_revision_entity_ids

                    self.call_indexer(entity)

                    # Reindex the rest of the entities in the list
                    for entity_entity_id in set(entity_ids):
                        # Retrieve the entity details
                        node = self.call_entity_api(entity_entity_id, 'entities')

                        self.call_indexer(node, True)

                logger.info("################reindex() DONE######################")

                # Clear the entity api cache
                self.entity_api_cache.clear()

                return "HuBMAP Translator.translate() finished executing"
        except Exception:
            msg = "Exceptions during executing indexer.reindex()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def update(self, entity_id, document):
        for index in self.indices.keys():
            public_index = self.INDICES['indices'][index]['public']
            private_index = self.INDICES['indices'][index]['private']

            if self.is_public(document):
                self.indexer.index(entity_id, json.dumps(document), public_index, True)

            self.indexer.index(entity_id, json.dumps(document), private_index, True)

    def add(self, entity_id, document):
        for index in self.indices.keys():
            public_index = self.INDICES['indices'][index]['public']
            private_index = self.INDICES['indices'][index]['private']

            if self.is_public(document):
                self.indexer.index(entity_id, json.dumps(document), public_index, False)

            self.indexer.index(entity_id, json.dumps(document), private_index, False)

    # Collection doesn't actually have this `data_access_level` property
    # This method is only applied to Donor/Sample/Dataset
    # For Dataset, if status=='Published', it goes into the public index
    # For Donor/Sample, `data`if any dataset down in the tree is 'Published', they should have `data_access_level` as public,
    # then they go into public index
    # Don't confuse with `data_access_level`
    def is_public(self, document):
        is_public = False

        if document['entity_type'] == 'Dataset':
            # In case 'status' not set
            if 'status' in document:
                if document['status'].lower() == self.DATASET_STATUS_PUBLISHED:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(
                    f"{document['entity_type']} of uuid: {document['uuid']} missing 'status' property, treat as not public, verify and set the status.")
        else:
            # In case 'data_access_level' not set
            if 'data_access_level' in document:
                if document['data_access_level'].lower() == self.ACCESS_LEVEL_PUBLIC:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(
                    f"{document['entity_type']} of uuid: {document['uuid']} missing 'data_access_level' property, treat as not public, verify and set the data_access_level.")

        return is_public

    def delete(self, entity_id):
        for index, _ in self.indices.items():
            # each index should have a public/private index
            public_index = self.INDICES['indices'][index]['public']
            self.indexer.delete_document(entity_id, public_index)

            private_index = self.INDICES['indices'][index]['private']
            if public_index != private_index:
                self.indexer.delete_document(entity_id, private_index)

    # When indexing, Upload WILL NEVER BE PUBLIC
    def translate_upload(self, entity_id, reindex=False):
        try:
            default_private_index = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['private']

            # Retrieve the upload entity details
            upload = self.call_entity_api(entity_id, 'entities')

            self.add_datasets_to_entity(upload)
            self.entity_keys_rename(upload)

            # Add additional calculated fields if any applies to Upload
            self.add_calculated_fields(upload)

            self.call_indexer(upload, reindex, json.dumps(upload), default_private_index)
        except Exception as e:
            logger.error(e)

    def translate_public_collection(self, entity_id, reindex=False):
        try:
            # The entity-api returns public collection with a list of connected public/published datasets, for either
            # - a valid token but not in HuBMAP-Read group or
            # - no token at all
            # Here we do NOT send over the token
            try:
                collection = self.get_public_collection(entity_id)
            except requests.exceptions.RequestException as e:
                logger.exception(e)
                # Stop running

                msg = "HubMAP Translator.translate_public_collection() failed to get public collection of uuid: {entity_id} via entity-api"
                logger.error(msg)
                sys.exit(msg)

            self.add_datasets_to_entity(collection)
            self.entity_keys_rename(collection)

            # Add additional calculated fields if any applies to Collection
            self.add_calculated_fields(collection)

            for index in self.indices.keys():
                # each index should have a public index
                public_index = self.INDICES['indices'][index]['public']
                private_index = self.INDICES['indices'][index]['private']

                # Add the tranformed doc to the portal index
                json_data = ""

                # if the index has a transformer use that else do a now load
                if self.TRANSFORMERS.get(index):
                    json_data = json.dumps(self.TRANSFORMERS[index].transform(collection))
                else:
                    json_data = json.dumps(collection)

                self.call_indexer(collection, reindex, json_data, public_index)
                self.call_indexer(collection, reindex, json_data, private_index)
        except Exception as e:
            logger.error(e)

    def translate_tree(self, entity_id):
        try:
            # logger.info(f"Total threads count: {threading.active_count()}")

            logger.info(f"Executing index_tree() for donor of uuid: {entity_id}")

            descendant_uuids = self.call_entity_api(entity_id, 'descendants', 'uuid')

            # Index the donor entity itself separately
            donor = self.call_entity_api(entity_id, 'entities')

            self.call_indexer(donor)

            # Index all the descendants of this donor
            for descendant_uuid in descendant_uuids:
                # Retrieve the entity details
                descendant = self.call_entity_api(descendant_uuid, 'entities')

                self.call_indexer(descendant)

            msg = f"indexer.index_tree() finished executing for donor of uuid: {entity_id}"
            logger.info(msg)
            return msg
        except Exception as e:
            logger.error(e)

    def init_transformers(self):
        for index in self.indices.keys():
            try:
                xform_module = self.INDICES['indices'][index]['transform']['module']

                logger.info(f"Transform module to be dynamically imported: {xform_module} at time: {time.time()}")

                try:
                    m = importlib.import_module(xform_module)
                    self.TRANSFORMERS[index] = m
                except Exception as e:
                    logger.error(e)
                    msg = f"Failed to dynamically import transform module index: {index} at time: {time.time()}"
                    logger.exception(msg)
            except KeyError as e:
                logger.info(f'No transform or transform module specified in the search-config.yaml for index: {index}')

        logger.debug("========Preloaded transformers===========")
        logger.debug(self.TRANSFORMERS)

    def init_auth_helper(self):
        if AuthHelper.isInitialized() == False:
            auth_helper = AuthHelper.create(self.app_client_id, self.app_client_secret)
        else:
            auth_helper = AuthHelper.instance()

        return auth_helper

    # Create a dict with HTTP Authorization header with Bearer token
    def create_request_headers_for_auth(self, token):
        auth_header_name = 'Authorization'
        auth_scheme = 'Bearer'

        headers_dict = {
            # Don't forget the space between scheme and the token value
            auth_header_name: auth_scheme + ' ' + token
        }

        return headers_dict

    def call_indexer(self, entity, reindex=False, document=None, target_index=None):
        try:
            if document is None:
                document = self.generate_doc(entity, 'json')

            if target_index:
                self.indexer.index(entity['uuid'], document, target_index, reindex)
            elif entity['entity_type'] == 'Upload':
                target_index = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['private']

                self.indexer.index(entity['uuid'], document, target_index, reindex)
            else:
                # write entity into indices
                for index in self.indices.keys():
                    public_index = self.INDICES['indices'][index]['public']
                    private_index = self.INDICES['indices'][index]['private']

                    # check to see if the index has a transformer, default to None if not found
                    transformer = self.TRANSFORMERS.get(index, None)

                    if self.is_public(entity):
                        public_doc = self.generate_public_doc(entity)

                        if transformer is not None:
                            public_transformed = transformer.transform(json.loads(public_doc))
                            public_transformed_doc = json.dumps(public_transformed)
                            target_doc = public_transformed_doc
                        else:
                            target_doc = public_doc

                        self.indexer.index(entity['uuid'], target_doc, public_index, reindex)

                    # add it to private
                    if transformer is not None:
                        private_transformed = transformer.transform(json.loads(document))
                        target_doc = json.dumps(private_transformed)
                    else:
                        target_doc = document

                    self.indexer.index(entity['uuid'], target_doc, private_index, reindex)
        except Exception:
            msg = f"Exception encountered during executing HuBMAPTranslator call_indexer() for uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)


    # The added fields specified in `entity_properties_list` should not be added
    # to themselves as sub fields
    # The `except_properties_list` is a subset of entity_properties_list
    def exclude_added_top_level_properties(self, entity_dict, except_properties_list = []):
        for prop in entity_properties_list:
            if (prop in entity_dict) and (prop not in except_properties_list):
                 entity_dict.pop(prop)


    # Used for Upload and Collection index
    def add_datasets_to_entity(self, entity):
        datasets = []
        if 'datasets' in entity:
            for dataset in entity['datasets']:
                # Retrieve the entity details
                dataset = self.call_entity_api(dataset['uuid'], 'entities')

                dataset_doc = self.generate_doc(dataset, 'dict')

                # dataset_doc.pop('ancestors')
                # dataset_doc.pop('ancestor_ids')
                # dataset_doc.pop('descendants')
                # dataset_doc.pop('descendant_ids')
                # dataset_doc.pop('immediate_descendants')
                # dataset_doc.pop('immediate_ancestors')
                # dataset_doc.pop('donor')
                # dataset_doc.pop('origin_sample')
                # dataset_doc.pop('source_sample')

                # This function call is equivalent to the above lines commented out
                # We probably don't need to except 'datasets' property because 
                # Dataset has no such property ever defined in entity schema yaml? - 7/22/2022 Zhou
                self.exclude_added_top_level_properties(dataset_doc, except_properties_list = ['metadata', 'files', 'datasets'])

                datasets.append(dataset_doc)

        entity['datasets'] = datasets

    def entity_keys_rename(self, entity):
        # logger.debug("==================entity before renaming keys==================")
        # logger.debug(entity)

        to_delete_keys = []
        temp = {}

        for key in entity:
            to_delete_keys.append(key)
            if key in self.attr_map['ENTITY']:
                # Special case of Sample.rui_location
                # To be backward compatible for API clients relying on the old version
                # Also gives the ES consumer flexibility to change the inner structure
                # Note: when `rui_location` is stored as json object (Python dict) in ES
                # with the default dynamic mapping, it can cause errors due to
                # the changing data types of some internal fields
                # isinstance() check is to avoid json.dumps() on json string again
                if (key == 'rui_location') and isinstance(entity[key], dict):
                    # Convert Python dict to json string
                    temp_val = json.dumps(entity[key])
                else:
                    temp_val = entity[key]

                temp[self.attr_map['ENTITY'][key]['es_name']] = temp_val

        for key in to_delete_keys:
            if key not in entity_properties_list:
                entity.pop(key)

        entity.update(temp)

        # logger.debug("==================entity after renaming keys==================")
        # logger.debug(entity)

    # These calculated fields are not stored in neo4j but will be generated
    # and added to the ES
    def add_calculated_fields(self, entity):
        # Add index_version by parsing the VERSION file
        entity['index_version'] = self.index_version

        # Add display_subtype
        if entity['entity_type'] in entity_types_with_display_subtype:
            entity['display_subtype'] = self.generate_display_subtype(entity)

    # For Upload, Dataset, Donor and Sample objects:
    # add a calculated (not stored in Neo4j) field called `display_subtype` to
    # all Elasticsearch documents of the above types with the following rules:
    # Upload: Just make it "Data Upload" for all uploads
    # Donor: "Donor"
    # Sample: if specimen_type == 'organ' the display name linked to the corresponding description of organ code
    # otherwise the display name linked to the value of the corresponding description of specimen_type code
    # Dataset: the display names linked to the values in data_types as a comma separated list
    def generate_display_subtype(self, entity):
        entity_type = entity['entity_type']
        display_subtype = '{unknown}'

        if entity_type == 'Upload':
            display_subtype = 'Data Upload'
        elif entity_type == 'Donor':
            display_subtype = 'Donor'
        elif entity_type == 'Sample':
            if 'specimen_type' in entity:
                if entity['specimen_type'].lower() == 'organ':
                    if 'organ' in entity:
                        display_subtype = get_type_description(entity['organ'], 'organ_types')
                    else:
                        logger.error(
                            f"Missing missing organ when specimen_type is set of Sample with uuid: {entity['uuid']}")
                else:
                    display_subtype = get_type_description(entity['specimen_type'], 'tissue_sample_types')
            else:
                logger.error(f"Missing specimen_type of Sample with uuid: {entity['uuid']}")
        elif entity_type == 'Dataset':
            if 'data_types' in entity:
                display_subtype = ','.join(entity['data_types'])
            else:
                logger.error(f"Missing data_types of Dataset with uuid: {entity['uuid']}")
        else:
            # Do nothing
            logger.error(
                f"Invalid entity_type: {entity_type}. Only generate display_subtype for Upload/Donor/Sample/Dataset")

        return display_subtype

    def generate_doc(self, entity, return_type):
        try:
            entity_id = entity['uuid']

            if entity['entity_type'] != 'Upload':
                ancestors = []
                descendants = []
                ancestor_ids = []
                descendant_ids = []
                immediate_ancestors = []
                immediate_descendants = []

                # Do not call /ancestors/<id> directly to avoid performance/timeout issue
                # Get back a list of ancestor uuids first
                ancestor_ids = self.call_entity_api(entity_id, 'ancestors', 'uuid')
                for ancestor_uuid in ancestor_ids:
                    # No need to call self.exclude_dataset_ingest_metadata_empty_fields() here because
                    # self.call_entity_api() already handled that
                    ancestor_dict = self.call_entity_api(ancestor_uuid, 'entities')
                    ancestors.append(ancestor_dict)

                # Find the Donor
                donor = None
                for a in ancestors:
                    if a['entity_type'] == 'Donor':
                        donor = copy.copy(a)
                        break

                # Get back a list of descendant uuids first
                descendant_ids = self.call_entity_api(entity_id, 'descendants', 'uuid')
                for descendant_uuid in descendant_ids:
                    # No need to call self.exclude_dataset_ingest_metadata_empty_fields() here because
                    # self.call_entity_api() already handled that
                    descendant_dict = self.call_entity_api(descendant_uuid, 'entities')
                    descendants.append(descendant_dict)

                # Calls to /parents/<id> and /children/<id> have no performance/timeout concerns
                immediate_ancestors_list = self.call_entity_api(entity_id, 'parents')
                for immediate_ancestor_dict in immediate_ancestors_list:
                    # We need to call self.exclude_dataset_ingest_metadata_empty_fields() here because
                    # self.call_entity_api() above returned a list of immediate ancestor dicts instead of uuids
                    # without excluding any Dataset.ingest_metadata.metadata sub fields with empty string values
                    immediate_ancestors.append(self.exclude_dataset_ingest_metadata_empty_fields(immediate_ancestor_dict))

                immediate_descendants_list = self.call_entity_api(entity_id, 'children')
                for immediate_descendant_dict in immediate_descendants_list:
                    # We need to call self.exclude_dataset_ingest_metadata_empty_fields() here because
                    # self.call_entity_api() above returned a list of immediate descendant dicts instead of uuids
                    # without excluding any Dataset.ingest_metadata.metadata sub fields with empty string values
                    immediate_descendants.append(self.exclude_dataset_ingest_metadata_empty_fields(immediate_descendant_dict))
                
                # Add new properties to entity
                entity['ancestors'] = ancestors
                entity['descendants'] = descendants

                entity['ancestor_ids'] = ancestor_ids
                entity['descendant_ids'] = descendant_ids

                entity['immediate_ancestors'] = immediate_ancestors
                entity['immediate_descendants'] = immediate_descendants

            # The origin_sample is the sample that `specimen_type` is "organ" and the `organ` code is set at the same time
            if entity['entity_type'] in ['Sample', 'Dataset']:
                # Add new properties
                entity['donor'] = donor

                entity['origin_sample'] = copy.copy(entity) if ('specimen_type' in entity) and (
                        entity['specimen_type'].lower() == 'organ') and ('organ' in entity) and (
                                                                       entity['organ'].strip() != '') else None
                if entity['origin_sample'] is None:
                    try:
                        # The origin_sample is the ancestor which `specimen_type` is "organ" and the `organ` code is set
                        entity['origin_sample'] = copy.copy(next(a for a in ancestors if ('specimen_type' in a) and (
                                a['specimen_type'].lower() == 'organ') and ('organ' in a) and (
                                                                         a['organ'].strip() != '')))
                    except StopIteration:
                        entity['origin_sample'] = {}

                # Remove those added fields specified in `entity_properties_list` from origin_sample and source_sample
                self.exclude_added_top_level_properties(entity['origin_sample'], except_properties_list = ['metadata'])

                # Trying to understand here!!!
                if entity['entity_type'] == 'Dataset':
                    entity['source_sample'] = None

                    e = entity

                    while entity['source_sample'] is None:
                        parents = self.call_entity_api(e['uuid'], 'parents')

                        try:
                            # Why?
                            if parents[0]['entity_type'] == 'Sample':
                                # entity['source_sample'] = parents[0]
                                entity['source_sample'] = parents

                            e = parents[0]
                        except IndexError:
                            entity['source_sample'] = {}

                    # Remove those added fields specified in `entity_properties_list` from origin_sample and source_sample
                    self.exclude_added_top_level_properties(entity['source_sample'], except_properties_list = ['metadata'])

                    # Move files to the root level if exist
                    entity['files'] = []
                    if 'ingest_metadata' in entity:
                        ingest_metadata = entity['ingest_metadata']
                        if 'files' in ingest_metadata and ((isinstance(ingest_metadata['files'], str) and ingest_metadata['files'].strip() != '') or not isinstance(ingest_metadata['files'], str)):
                            entity['files'] = ingest_metadata.pop('files')



            self.entity_keys_rename(entity)

            # Is group_uuid always set?
            # In case if group_name not set
            if ('group_uuid' in entity) and ('group_name' not in entity):
                group_uuid = entity['group_uuid']

                # Get the globus groups info based on the groups json file in commons package
                globus_groups_info = globus_groups.get_globus_groups_info()
                groups_by_id_dict = globus_groups_info['by_id']
                group_dict = groups_by_id_dict[group_uuid]

                # Add new property
                entity['group_name'] = group_dict['displayname']


            # Rename for properties that are objects
            if entity.get('donor', None):
                self.entity_keys_rename(entity['donor'])
            if entity.get('origin_sample', None):
                self.entity_keys_rename(entity['origin_sample'])
            if entity.get('source_sample', None):
                for s in entity.get('source_sample', None):
                    self.entity_keys_rename(s)
            if entity.get('ancestors', None):
                for a in entity.get('ancestors', None):
                    self.entity_keys_rename(a)
            if entity.get('descendants', None):
                for d in entity.get('descendants', None):
                    self.entity_keys_rename(d)
            if entity.get('immediate_descendants', None):
                for parent in entity.get('immediate_descendants', None):
                    self.entity_keys_rename(parent)
            if entity.get('immediate_ancestors', None):
                for child in entity.get('immediate_ancestors', None):
                    self.entity_keys_rename(child)

            remove_specific_key_entry(entity, "other_metadata")

            # Add additional calculated fields
            self.add_calculated_fields(entity)

            return json.dumps(entity) if return_type == 'json' else entity
        except Exception:
            msg = "Exceptions during executing indexer.generate_doc()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def generate_public_doc(self, entity):
        # Only Dataset has this 'next_revision_uuid' property
        property_key = 'next_revision_uuid'
        if (entity['entity_type'] == 'Dataset') and (property_key in entity):
            next_revision_uuid = entity[property_key]
            # Making a call against entity-api/entities/<next_revision_uuid>?property=status
            url = self.entity_api_url + "/entities/" + next_revision_uuid + "?property=status"
            response = requests.get(url, headers=self.request_headers, verify=False)

            if response.status_code != 200:
                msg = f"indexer.generate_public_doc() failed to get status of next_revision_uuid via entity-api for uuid: {next_revision_uuid}"
                logger.error(msg)
                sys.exit(msg)

            # The call to entity-api returns string directly
            dataset_status = (response.text).lower()

            # Check the `next_revision_uuid` and if the dataset is not published,
            # pop the `next_revision_uuid` from this entity
            if dataset_status != self.DATASET_STATUS_PUBLISHED:
                logger.debug(f"Remove the {property_key} property from {entity['uuid']}")
                entity.pop(property_key)

        entity['descendants'] = list(filter(self.is_public, entity['descendants']))
        entity['immediate_descendants'] = list(filter(self.is_public, entity['immediate_descendants']))
        return json.dumps(entity)

    # Remove any Dataset.ingest_metadata.metadata sub fields if the value is empty string or just whitespace 
    # to address the Elasticsearch index error due to inconsistent data types - 7/13/2022 Max & Zhou
    def exclude_dataset_ingest_metadata_empty_fields(self, dataset_dict):
        if ('ingest_metadata' in dataset_dict) and ('metadata' in dataset_dict['ingest_metadata']):
            for key in list(dataset_dict['ingest_metadata']['metadata']):
                if isinstance(dataset_dict['ingest_metadata']['metadata'][key], str):
                    if not dataset_dict['ingest_metadata']['metadata'][key] or re.search(r'^\s+$', dataset_dict['ingest_metadata']['metadata'][key]):
                        del dataset_dict['ingest_metadata']['metadata'][key]
                        logger.info(f"Removed ['ingest_metadata']['metadata']['{key}'] due to empty string value, for Dataset {dataset_dict['uuid']}")
        
        return dataset_dict


    def call_entity_api(self, entity_id, endpoint, url_property=None):
        url = self.entity_api_url + "/" + endpoint + "/" + entity_id
        if url_property:
            url += "?property=" + url_property

        if url in self.entity_api_cache:
            # Return the deepcopy of the original dict
            # Becase the cached dict will be reused by other calls and get modified by adding extra fields during index
            # Shallow copy will reference to the original dict which gets added with extra fields
            logger.debug(f"Returning the cached deepcopy of entity dict for uuid: {entity_id}")

            return copy.deepcopy(self.entity_api_cache[url])

        response = requests.get(url, headers=self.request_headers, verify=False)
        if response.status_code != 200:
            # See if this uuid is a public Collection instead before exiting
            try:
                return self.get_public_collection(entity_id)
            except requests.exceptions.RequestException as e:
                logger.exception(e)

                # Stop running
                msg = f"HuBMAP translator failed to reach: " + url + ". Response: " + response.json()
                logger.error(msg)
                sys.exit(msg)

        # Remove any Dataset.ingest_metadata.metadata sub fields if the value is empty string or just whitespace 
        # to address the Elasticsearch index error due to inconsistent data types
        # If entity is not Dataset, no change - 7/13/2022 Max & Zhou
        entity_dict = self.exclude_dataset_ingest_metadata_empty_fields(response.json())

        # Make a deepcopy of the entity dict to store in cache
        # Because this `entity_dict` will get modified by adding extra fields during index
        self.entity_api_cache[url] = copy.deepcopy(entity_dict)

        return entity_dict

    def get_public_collection(self, entity_id):
        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        url = self.entity_api_url + "/collections/" + entity_id
        response = requests.get(url, headers=self.request_headers, verify=False)

        if response.status_code != 200:
            msg = "HuBMAP translator get_collection() failed to get public collection of uuid: " + entity_id + " via entity-api"

            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            logger.debug("======get_public_collection() status code from hubmap_translator======")
            logger.debug(response.status_code)

            logger.debug("======get_public_collection() response text from hubmap_translator-api======")
            logger.debug(response.text)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        collection_dict = response.json()

        return collection_dict

    def main(self):
        try:
            # Delete and recreate target indices
            # for index, configs in self.indices['indices'].items():
            for index in self.indices.keys():
                # each index should have a public/private index
                public_index = self.INDICES['indices'][index]['public']
                private_index = self.INDICES['indices'][index]['private']

                try:
                    self.indexer.delete_index(public_index)
                except Exception as e:
                    pass

                try:
                    self.indexer.delete_index(private_index)
                except Exception as e:
                    pass

                # get the specific mapping file for the designated index
                index_mapping_file = self.INDICES['indices'][index]['elasticsearch']['mappings']

                # read the elasticserach specific mappings
                index_mapping_settings = safe_load((Path(__file__).absolute().parent / index_mapping_file).read_text())

                self.indexer.create_index(public_index, index_mapping_settings)
                self.indexer.create_index(private_index, index_mapping_settings)

        except Exception:
            msg = "Exception encountered during executing indexer.main()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)


# Running indexer_base.py as a script in command line
# This approach is different from the live reindex via HTTP request
# It'll delete all the existing indices and recreate then then index everything
if __name__ == "__main__":
    # Specify the absolute path of the instance folder and use the config file relative to the instance path
    app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), '../src/instance'),
                instance_relative_config=True)
    app.config.from_pyfile('app.cfg')

    INDICES = safe_load((Path(__file__).absolute().parent / 'instance/search-config.yaml').read_text())

    try:
        token = sys.argv[1]
    except IndexError as e:
        msg = "Missing admin group token argument"
        logger.exception(msg)
        sys.exit(msg)

    # Create an instance of the indexer
    translator = Translator(INDICES, app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], token)

    auth_helper = translator.init_auth_helper()

    # The second argument indicates to get the groups information
    user_info_dict = auth_helper.getUserInfo(token, True)

    if isinstance(user_info_dict, Response):
        msg = "The given token is expired or invalid"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    # Use the new key rather than the 'hmgroupids' which will be deprecated
    group_ids = user_info_dict['group_membership_ids']

    # Ensure the user belongs to the HuBMAP-Data-Admin group
    # TODO: Need to generalize this once HuBMAP authorization is updated
    if not user_belongs_to_data_admin_group(group_ids, app.config['GLOBUS_HUBMAP_DATA_ADMIN_GROUP_UUID']):
        msg = "The given token doesn't belong to the HuBMAP-Data-Admin group, access not granted"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    start = time.time()
    logger.info("############# Full index via script started #############")

    translator.main()
    translator.translate_all()

    end = time.time()
    logger.info(
        f"############# Full index via script completed. Total time used: {end - start} seconds. #############")
