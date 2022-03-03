import copy
import importlib
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests
import yaml
from hubmap_commons import globus_groups
# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper

from Indexer import Indexer
from translator.TranslatorInterface import TranslatorInterface

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

entity_properties_list = [
    'metadata',
    'source',
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
entity_types = ['Upload', 'Source', 'Sample', 'Dataset']


class SenNetTranslator(TranslatorInterface):
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    DATASET_STATUS_PUBLISHED = 'published'
    DEFAULT_INDEX_WITHOUT_PREFIX = ''
    INDICES = {}
    TRANSFORMERS = {}

    def translate_all(self):
        pass

    def translate(self, entity_id):
        try:
            # Retrieve the entity details
            entity = self.call_entity_api(entity_id, 'entities')

            # Check if entity is empty
            if bool(entity):
                logger.info(f"Executing translate() for entity_id: {entity_id}, entity_type: {entity['entity_type']}")

                if entity['entity_type'] == 'Collection':
                    self.translate_public_collection(entity, reindex=True)
                elif entity['entity_type'] == 'Upload':
                    self.translate_upload(entity, reindex=True)
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

                return "indexer.reindex() finished executing"
        except Exception:
            msg = "Exceptions during executing indexer.reindex()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def delete(self, entity_id):
        for index, _ in self.indices.items():
            # each index should have a public/private index
            public_index = self.INDICES['indices'][index]['public']
            Indexer.delete(entity_id, public_index)

            private_index = self.INDICES['indices'][index]['private']
            if public_index != private_index:
                Indexer.delete(entity_id, private_index)

    # When indexing, Upload WILL NEVER BE PUBLIC
    def translate_upload(self, entity, reindex=False):
        default_private_index = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['private']

        # Retrieve the upload entity details
        upload = self.call_entity_api(entity['uuid'], 'entities')

        self.add_datasets_to_entity(upload)
        self.entity_keys_rename(upload)

        # Add additional calculated fields if any applies to Upload
        self.add_calculated_fields(upload)

        self.call_indexer(entity, reindex, json.dumps(upload), default_private_index)

    def translate_public_collection(self, entity, reindex=False):
        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        collection = self.call_entity_api(entity['uuid'], 'collections')

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

            self.call_indexer(entity, reindex, json_data, public_index)
            self.call_indexer(entity, reindex, json_data, private_index)

    def translate_tree(self, entity_id):
        # logger.info(f"Total threads count: {threading.active_count()}")

        logger.info(f"Executing index_tree() for source of uuid: {entity_id}")

        descendant_uuids = self.call_entity_api(entity_id, 'descendants', 'uuid')

        # Index the source entity itself separately
        source = self.call_entity_api(entity_id, 'entities')

        self.call_indexer(source)

        # Index all the descendants of this source
        for descendant_uuid in descendant_uuids:
            # Retrieve the entity details
            descendant = self.call_entity_api(descendant_uuid, 'entities')

            self.call_indexer(descendant)

        msg = f"indexer.index_tree() finished executing for source of uuid: {entity_id}"
        logger.info(msg)
        return msg

    def __init__(self, indices, app_client_id, app_client_secret, token):
        try:
            self.indices: dict = {}
            # Do not include the indexes that are self managed...
            for key, value in indices['indices'].items():
                if 'reindex_enabled' in value and value['reindex_enabled'] is True:
                    self.indices[key] = value
            self.DEFAULT_INDEX_WITHOUT_PREFIX: str = indices['default_index']
            self.INDICES: dict = {'default_index': self.DEFAULT_INDEX_WITHOUT_PREFIX, 'indices': self.indices}
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
        self.index_version = ((Path(__file__).absolute().parent.parent.parent / 'VERSION').read_text()).strip()

        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               'sennet_translation/neo4j-to-es-attributes.json'),
                  'r') as json_file:
            self.attr_map = json.load(json_file)

        # # Preload all the transformers
        self.init_transformers()

    def init_transformers(self):
        for index in self.indices.keys():
            try:
                xform_module = self.INDICES['indices'][index]['transform']['module']
                m = importlib.import_module(xform_module)
                self.TRANSFORMERS[index] = m
            except Exception as e:
                msg = f"Transformer missing or not specified for index: {index}"
                logger.info(msg)

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
            org_node = copy.deepcopy(entity)
            indexer = Indexer(self.indices, self.DEFAULT_INDEX_WITHOUT_PREFIX)

            if document is None:
                document = self.generate_doc(entity, 'json')

            if target_index:
                Indexer.index(entity['uuid'], document, target_index, reindex)
            elif entity['entity_type'] == 'Upload':
                target_index = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['private']

                Indexer.index(entity['uuid'], document, target_index, reindex)
            else:
                # write entity into indices
                for index in self.indices.keys():
                    public_index = self.INDICES['indices'][index]['public']
                    private_index = self.INDICES['indices'][index]['private']

                    # check to see if the index has a transformer, default to None if not found
                    transformer = self.TRANSFORMERS.get(index, None)

                    if self.entity_is_public(org_node):
                        public_doc = self.generate_public_doc(entity)

                        if transformer is not None:
                            public_transformed = transformer.transform(json.loads(public_doc))
                            public_transformed_doc = json.dumps(public_transformed)
                            target_doc = public_transformed_doc
                        else:
                            target_doc = public_doc

                        indexer.index(entity['uuid'], target_doc, public_index, reindex)

                    # add it to private
                    if transformer is not None:
                        private_transformed = transformer.transform(json.loads(document))
                        target_doc = json.dumps(private_transformed)
                    else:
                        target_doc = document

                    indexer.index(entity['uuid'], target_doc, private_index, reindex)
        except Exception:
            msg = f"Exception encountered during executing SenNetTranslator call_indexer() for uuid: {org_node['uuid']}, entity_type: {org_node['entity_type']}"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def call_entity_api(self, entity_id, endpoint, url_property=None):
        url = self.entity_api_url + "/" + endpoint + "/" + entity_id
        if url_property:
            url += "?property=" + url_property

        response = requests.get(url, headers=self.request_headers, verify=False)
        if response.status_code != 200:
            msg = f"SenNet translator failed to get " + endpoint + " via entity-api for target entity_id: " + entity_id
            logger.error(msg)
            sys.exit(msg)

        return response.json()

    def add_datasets_to_entity(self, entity):
        datasets = []
        if 'datasets' in entity:
            for dataset in entity['datasets']:
                # Retrieve the entity details
                dataset = self.call_entity_api(dataset['uuid'], 'entities')

                dataset_doc = self.generate_doc(dataset, 'dict')
                dataset_doc.pop('ancestors')
                dataset_doc.pop('ancestor_ids')
                dataset_doc.pop('descendants')
                dataset_doc.pop('descendant_ids')
                dataset_doc.pop('immediate_descendants')
                dataset_doc.pop('immediate_ancestors')
                dataset_doc.pop('source')
                dataset_doc.pop('origin_sample')
                dataset_doc.pop('source_sample')

                datasets.append(dataset_doc)

        entity['datasets'] = datasets

    def entity_keys_rename(self, entity):
        to_delete_keys = []
        temp = {}

        for key in entity:
            to_delete_keys.append(key)
            if key in self.attr_map['ENTITY']:
                temp_val = entity[key]
                temp[self.attr_map['ENTITY'][key]['es_name']] = temp_val

        for key in to_delete_keys:
            if key not in entity_properties_list:
                entity.pop(key)

        entity.update(temp)

    def remove_specific_key_entry(self, obj, key_to_remove):
        if type(obj) == dict:
            if key_to_remove in obj.keys():
                obj.pop(key_to_remove)

            for key in obj.keys():
                self.remove_specific_key_entry(obj[key], key_to_remove)
        elif type(obj) == list:
            for e in obj:
                self.remove_specific_key_entry(e, key_to_remove)

    # These calculated fields are not stored in neo4j but will be generated
    # and added to the ES
    def add_calculated_fields(self, entity):
        # Add index_version by parsing the VERSION file
        entity['index_version'] = self.index_version

        # Add display_subtype
        if entity['entity_type'] in entity_types:
            entity['display_subtype'] = self.generate_display_subtype(entity)

    # For Upload, Dataset, Source and Sample objects:
    # add a calculated (not stored in Neo4j) field called `display_subtype` to
    # all Elasticsearch documents of the above types with the following rules:
    # Upload: Just make it "Data Upload" for all uploads
    # Source: "Source"
    # Sample: if specimen_type == 'organ' the display name linked to the corresponding description of organ code
    # otherwise the display name linked to the value of the corresponding description of specimen_type code
    # Dataset: the display names linked to the values in data_types as a comma separated list
    def generate_display_subtype(self, entity):
        entity_type = entity['entity_type']
        display_subtype = '{unknown}'

        if entity_type == 'Upload':
            display_subtype = 'Data Upload'
        elif entity_type == 'Source':
            display_subtype = 'Source'
        elif entity_type == 'Sample':
            if 'specimen_type' in entity:
                if entity['specimen_type'].lower() == 'organ':
                    if 'organ' in entity:
                        display_subtype = self.get_type_description(entity['organ'], 'organ_types')
                    else:
                        logger.error(
                            f"Missing missing organ when specimen_type is set of Sample with uuid: {entity['uuid']}")
                else:
                    display_subtype = self.get_type_description(entity['specimen_type'], 'tissue_sample_types')
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
                f"Invalid entity_type: {entity_type}. Only generate display_subtype for Upload/Source/Sample/Dataset")

        return display_subtype

    def get_type_description(self, type_code, type_yaml_file_name):
        filename = 'search-schema/data/definitions/enums/' + type_yaml_file_name + '.yaml'
        type_yaml_file = Path(
            __file__).absolute().parent.parent / filename

        logger.debug(f"========type_code: {type_code}")

        with open(type_yaml_file) as file:
            definition_dict = yaml.safe_load(file)

            logger.info(f"Definition yaml file {type_yaml_file} loaded successfully")

            if type_code in definition_dict:
                definition_desc = definition_dict[type_code]['description']
            else:
                # Return the error message as description
                msg = f"Missing definition key {type_code} in {type_yaml_file}"

                logger.error(msg)

                # Use triple {{{}}}
                definition_desc = f"{{{type_code}}}"

            logger.debug(f"========definition_desc: {definition_desc}")

            return definition_desc

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
                ancestor_ids = self.call_entity_api(entity_id, 'ancestors', 'uuid')

                for ancestor_uuid in ancestor_ids:
                    # Retrieve the entity details
                    ancestor_dict = self.call_entity_api(ancestor_uuid, 'entities')

                    # Add to the list
                    ancestors.append(ancestor_dict)

                # Find the Source
                source = None
                for a in ancestors:
                    if a['entity_type'] == 'Source':
                        source = copy.copy(a)
                        break

                descendant_ids = self.call_entity_api(entity_id, 'descendants', 'uuid')

                for descendant_uuid in descendant_ids:
                    # Retrieve the entity details
                    descendant_dict = self.call_entity_api(descendant_uuid, 'entities')

                    # Add to the list
                    descendants.append(descendant_dict)

                # Calls to /parents/<id> and /children/<id> have no performance/timeout concerns
                immediate_ancestors = self.call_entity_api(entity_id, 'parents')
                immediate_descendants = self.call_entity_api(entity_id, 'children')

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
                entity['source'] = source

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

                    # Move files to the root level if exist
                    if 'ingest_metadata' in entity:
                        ingest_metadata = entity['ingest_metadata']
                        if 'files' in ingest_metadata:
                            entity['files'] = ingest_metadata['files']

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

            # Remove the `files` element from the entity['metadata'] dict
            # to reduce the doc size to be indexed?
            if ('metadata' in entity) and ('files' in entity['metadata']):
                entity['metadata'].pop('files')

            # Rename for properties that are objects
            if entity.get('source', None):
                self.entity_keys_rename(entity['source'])
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

            self.remove_specific_key_entry(entity, "other_metadata")

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

        entity['descendants'] = list(filter(self.entity_is_public, entity['descendants']))
        entity['immediate_descendants'] = list(filter(self.entity_is_public, entity['immediate_descendants']))
        return json.dumps(entity)

    # Collection doesn't actually have this `data_access_level` property
    # This method is only applied to Source/Sample/Dataset
    # For Dataset, if status=='Published', it goes into the public index
    # For Source/Sample, `data`if any dataset down in the tree is 'Published', they should have `data_access_level` as public,
    # then they go into public index
    # Don't confuse with `data_access_level`
    def entity_is_public(self, node):
        is_public = False

        if node['entity_type'] == 'Dataset':
            # In case 'status' not set
            if 'status' in node:
                if node['status'].lower() == self.DATASET_STATUS_PUBLISHED:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(
                    f"{node['entity_type']} of uuid: {node['uuid']} missing 'status' property, treat as not public, verify and set the status.")
        else:
            # In case 'data_access_level' not set
            if 'data_access_level' in node:
                if node['data_access_level'].lower() == self.ACCESS_LEVEL_PUBLIC:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(
                    f"{node['entity_type']} of uuid: {node['uuid']} missing 'data_access_level' property, treat as not public, verify and set the data_access_level.")

        return is_public
