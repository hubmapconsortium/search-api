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
    'donor',
    'origin_samples',
    'source_samples',
    'ancestor_ids',
    'descendant_ids',
    'ancestors',
    'descendants',
    # This 'files' field is either empty list [] or the files info list copied from 'Dataset.ingest_metadata.files'
    'files',
    'datasets',
    'immediate_ancestors',
    'immediate_descendants'
]

# Entity types that will have `display_subtype` generated ar index time
entity_types_with_display_subtype = ['Upload', 'Donor', 'Sample', 'Dataset', 'Publication']


class Translator(TranslatorInterface):
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    DATASET_STATUS_PUBLISHED = 'published'
    DEFAULT_INDEX_WITHOUT_PREFIX = ''
    INDICES = {}
    TRANSFORMERS = {}
    DEFAULT_ENTITY_API_URL = ''
    indexer = None
    skip_comparision = False
    failed_entity_api_calls = []
    failed_entity_ids = []


    def __init__(self, indices, app_client_id, app_client_secret, token):
        try:
            self.indices: dict = {}
            self.self_managed_indices: dict = {}
            # Do not include the indexes that are self managed
            for key, value in indices['indices'].items():
                if 'reindex_enabled' in value and value['reindex_enabled'] is True:
                    self.indices[key] = value
                else:
                    self.self_managed_indices[key] = value
            self.DEFAULT_INDEX_WITHOUT_PREFIX: str = indices['default_index']
            self.INDICES: dict = {'default_index': self.DEFAULT_INDEX_WITHOUT_PREFIX, 'indices': self.indices}
            self.DEFAULT_ENTITY_API_URL = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')

            self.indexer = Indexer(self.indices, self.DEFAULT_INDEX_WITHOUT_PREFIX)

            logger.debug("=========== INDICES config ===========")
            logger.debug(self.INDICES)
        except Exception:
            raise ValueError("Invalid indices config")

        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token
        self.request_headers = self.create_request_headers_for_auth(token)
        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')
        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent / 'VERSION').read_text()).strip()

        with open(Path(__file__).resolve().parent / 'hubmap_translation' / 'neo4j-to-es-attributes.json', 'r') as json_file:
            self.attr_map = json.load(json_file)

        # # Preload all the transformers
        self.init_transformers()


    # Used by full reindex via script and live reindex-all call
    def translate_all(self):
        with app.app_context():
            try:
                logger.info("Start executing translate_all()")

                start = time.time()

                donor_uuids_list = get_uuids_by_entity_type("donor", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                upload_uuids_list = get_uuids_by_entity_type("upload", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                public_collection_uuids_list = get_uuids_by_entity_type("collection", self.request_headers, self.DEFAULT_ENTITY_API_URL)

                # Only need this comparision for the live /rindex-all PUT call
                if not self.skip_comparision:
                    # Make calls to entity-api to get a list of uuids for rest of entity types
                    sample_uuids_list = get_uuids_by_entity_type("sample", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                    dataset_uuids_list = get_uuids_by_entity_type("dataset", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                    
                    # Merge into a big list that with no duplicates
                    all_entities_uuids = set(donor_uuids_list + sample_uuids_list + dataset_uuids_list + upload_uuids_list + public_collection_uuids_list)

                    es_uuids = []
                    index_names = get_all_reindex_enabled_indice_names(self.INDICES)

                    for index in index_names.keys():
                        all_indices = index_names[index]
                        # get URL for that index
                        es_url = self.INDICES['indices'][index]['elasticsearch']['url'].strip('/')

                        for actual_index in all_indices:
                            es_uuids.extend(get_uuids_from_es(actual_index, es_url))

                    es_uuids = set(es_uuids)

                    # Remove entities found in Elasticsearch but no longer in neo4j
                    for uuid in es_uuids:
                        if uuid not in all_entities_uuids:
                            logger.debug(f"Entity of uuid: {uuid} found in Elasticsearch but no longer in neo4j. Delete it from Elasticsearch.")
                            self.delete(uuid)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    # The default number of threads in the ThreadPoolExecutor is calculated as: 
                    # From 3.8 onwards default value is min(32, os.cpu_count() + 4)
                    # Where the number of CPUs is determined by Python and will take hyperthreading into account
                    logger.info(f"The number of worker threads being used by default: {executor._max_workers}")

                    # Submit tasks to the thread pool
                    public_collection_futures_list = [executor.submit(self.translate_public_collection, uuid, reindex=True) for uuid in public_collection_uuids_list]
                    upload_futures_list = [executor.submit(self.translate_upload, uuid, reindex=True) for uuid in upload_uuids_list]

                    # Append the above lists into one
                    futures_list = public_collection_futures_list + upload_futures_list

                    # The target function runs the task logs more details when f.result() gets executed
                    for f in concurrent.futures.as_completed(futures_list):
                        result = f.result()

                # Index the donor tree in a regular for loop, not the concurrent mode
                # However, the descendants of a given donor will be indexed concurrently
                for uuid in donor_uuids_list:
                    self.translate_donor_tree(uuid)

                end = time.time()

                logger.info(f"Finished executing translate_all(). Total time used: {end - start} seconds.")
            except Exception as e:
                logger.error(e)

    def __get_scope_list(self, entity_id, document, index, scope):
        scope_list = []
        if index == 'files':
            # It would be nice if the possible scopes could be identified from
            # self.INDICES['indices'] rather than hardcoded. @TODO
            # This can handle indices besides "files" which might accept "scope" as
            # an argument, but returning an empty list, not raising an Exception, for
            # an  unrecognized index name.
            if scope is not None:
                if scope not in ['public', 'private']:
                    msg = (f"Unrecognized scope '{scope}' requested for"
                           f" entity_id '{entity_id}' in Dataset '{document['dataset_uuid']}.")
                    logger.info(msg)
                    raise ValueError(msg)
                elif scope == 'public':
                    if self.is_public(document):
                        scope_list.append(scope)
                    else:
                        # Reject the addition of 'public' was explicitly indicated, even though
                        # the public index may be silently skipped when a scope is not specified, in
                        # order to mimic behavior below for "non-self managed" indices.
                        msg = (f"Dataset '{document['dataset_uuid']}"
                               f" does not have status {self.DATASET_STATUS_PUBLISHED}, so"
                               f" entity_id '{entity_id}' cannot go in a public index.")
                        logger.info(msg)
                        raise ValueError(msg)
                elif scope == 'private':
                    scope_list.append(scope)
            else:
                scope_list = ['public', 'private']
        return scope_list


    # Used by individual live reindex call
    def translate(self, entity_id):
        try:
            # Retrieve the entity details
            # This returned entity dict (if Dataset) has removed ingest_metadata.files and
            # ingest_metadata.metadata sub fields with empty string values when call_entity_api() gets called
            entity = self.call_entity_api(entity_id, 'entities')

            logger.info(f"Start executing translate() on {entity['entity_type']} of uuid: {entity_id}")

            if entity['entity_type'] == 'Collection':
                self.translate_public_collection(entity_id, reindex=True)
            elif entity['entity_type'] == 'Upload':
                self.translate_upload(entity_id, reindex=True)
            else:
                # Reindex the entity itself first
                self.call_indexer(entity, reindex=True)

                previous_revision_ids = []
                next_revision_ids = []

                ancestor_ids = self.call_entity_api(entity_id, 'ancestors', 'uuid')
                descendant_ids = self.call_entity_api(entity_id, 'descendants', 'uuid')

                # Only Dataset/Publication entities may have previous/next revisions
                if entity['entity_type'] in ['Dataset', 'Publication']:
                    previous_revision_ids = self.call_entity_api(entity_id, 'previous_revisions', 'uuid')
                    next_revision_ids = self.call_entity_api(entity_id, 'next_revisions', 'uuid')

                # All unique entity ids in the path excluding the entity itself
                target_ids = set(ancestor_ids + descendant_ids + previous_revision_ids + next_revision_ids)

                # Reindex the rest of the entities in the list
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures_list = [executor.submit(self.reindex_entity, uuid) for uuid in target_ids]
                    for f in concurrent.futures.as_completed(futures_list):
                        result = f.result()

                logger.info(f"Finished executing translate() on {entity['entity_type']} of uuid: {entity_id}")
        except Exception:
            msg = "Exceptions during executing translate()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def update(self, entity_id, document, index=None, scope=None):
        if index is not None and index == 'files':
            # The "else clause" is the dominion of the original flavor of OpenSearch indices, for which search-api
            # was created.  This clause is specific to 'files' indices, by virtue of the conditions and the
            # following assumption that dataset_uuid is on the JSON body. @TODO-KBKBKB right?
            scope_list = self.__get_scope_list(entity_id, document, index, scope)

            response = ''
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if scope == 'public' and not self.is_public(document):
                    # Mimic behavior of "else:" clause for "non-self managed" indices below, and
                    # silently skip public if it was put on the list by __get_scope_list() because
                    # the scope was not explicitly specified.
                    continue
                response += self.indexer.index(entity_id, json.dumps(document), target_index, True)
                response += '. '
        else:
            for index in self.indices.keys():
                public_index = self.INDICES['indices'][index]['public']
                private_index = self.INDICES['indices'][index]['private']

                if self.is_public(document):
                    response = self.indexer.index(entity_id, json.dumps(document), public_index, True)

                response += self.indexer.index(entity_id, json.dumps(document), private_index, True)
        return response

    def add(self, entity_id, document, index=None, scope=None):
        if index is not None and index == 'files':
            # The "else clause" is the dominion of the original flavor of OpenSearch indices, for which search-api
            # was created.  This clause is specific to 'files' indices, by virtue of the conditions and the
            # following assumption that dataset_uuid is on the JSON body. @TODO-KBKBKB right?
            scope_list = self.__get_scope_list(entity_id, document, index, scope)

            response = ''
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if scope == 'public' and not self.is_public(document):
                    # Mimic behavior of "else:" clause for "non-self managed" indices below, and
                    # silently skip public if it was put on the list by __get_scope_list() because
                    # the scope was not explicitly specified.
                    continue
                response += self.indexer.index(entity_id, json.dumps(document), target_index, False)
                response += '. '
        else:
            for index in self.indices.keys():
                public_index = self.INDICES['indices'][index]['public']
                private_index = self.INDICES['indices'][index]['private']

                if self.is_public(document):
                    response = self.indexer.index(entity_id, json.dumps(document), public_index, False)

                response += self.indexer.index(entity_id, json.dumps(document), private_index, False)
        return response

    # Collection doesn't actually have this `data_access_level` property
    # This method is only applied to Donor/Sample/Dataset/File
    # For File, if the Dataset of the dataset_uuid element has status=='Published', it may go in a public index
    # For Dataset, if status=='Published', it goes into the public index
    # For Donor/Sample, `data`if any dataset down in the tree is 'Published', they should have `data_access_level` as public,
    # then they go into public index
    # Don't confuse with `data_access_level`
    def is_public(self, document):
        is_public = False

        if 'file_uuid' in document:
            # Confirm the Dataset to which the File entity belongs is published
            dataset = self.call_entity_api(document['dataset_uuid'], 'entities')
            return self.is_public(dataset)

        if document['entity_type'] in ['Dataset', 'Publication']:
            # In case 'status' not set
            if 'status' in document:
                if document['status'].lower() == self.DATASET_STATUS_PUBLISHED:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(f"{document['entity_type']} of uuid: {document['uuid']} missing 'status' property, treat as not public, verify and set the status.")
        else:
            # In case 'data_access_level' not set
            if 'data_access_level' in document:
                if document['data_access_level'].lower() == self.ACCESS_LEVEL_PUBLIC:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(f"{document['entity_type']} of uuid: {document['uuid']} missing 'data_access_level' property, treat as not public, verify and set the data_access_level.")

        return is_public

    def delete_docs(self, index, scope, entity_id):
        # Clear multiple documents from the OpenSearch indices associated with the composite index specified
        # When index is for the files-api and entity_id is for a File, clear all file manifests for the File.
        # When index is for the files-api and entity_id is for a Dataset, clear all file manifests for the Dataset.
        # When index is for the files-api and entity_id is not specified, clear all file manifests in the index.
        # Otherwise, raise an Exception indicating the specified arguments are not supported.

        if not index:
            # Shouldn't happen due to configuration of Flask Blueprint routes
            raise ValueError(f"index must be specified for delete_docs()")

        if index == 'files':
            # For deleting documents, try removing them from the specified scope, but do not
            # raise any Exception or return an error response if they are not there to be deleted.
            scope_list = [scope] if scope else ['public', 'private']

            if entity_id:
                try:
                    # Get the Dataset entity with the specified entity_id
                    theEntity = self.call_entity_api(entity_id, 'entities')
                except Exception as e:
                    # entity-api may throw an Exception if entity_id is actually the
                    # uuid of a File, so swallow the error here and process as
                    # removing the file info document for a File below
                    logger.info(    f"No entity found  with entity_id '{entity_id}' in Neo4j, so process as"
                                    f" a request to delete a file info document for a File with that UUID.")
                    theEntity = {   'entity_type': 'File'
                                    ,'uuid': entity_id}

            response = ''
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if entity_id:
                    # Confirm the found entity for entity_id is of a supported type.  This probably repeats
                    # work done by the caller, but count on the caller for other business logic, like constraining
                    # to Datasets without PHI.
                    if theEntity and theEntity['entity_type'] not in ['Dataset',  'Publication', 'File']:
                        raise ValueError(   f"Translator.delete_docs() is not configured to clear documents for"
                                            f" entities of type '{theEntity['entity_type']} for HuBMAP.")
                    elif theEntity['entity_type'] in ['Dataset', 'Publication']:
                        try:
                            resp = self.indexer.delete_fieldmatch_document( target_index
                                                                            ,'dataset_uuid'
                                                                            , theEntity['uuid'])
                            response += resp[0]
                        except Exception as e:
                            response += (f"While deleting the Dataset '{theEntity['uuid']}' file info documents"
                                         f" from {target_index},"
                                         f" exception raised was {str(e)}.")
                    elif theEntity['entity_type'] == 'File':
                        try:
                            resp = self.indexer.delete_fieldmatch_document( target_index
                                                                            ,'file_uuid'
                                                                            ,theEntity['uuid'])
                            response += resp[0]
                        except Exception as e:
                            response += (   f"While deleting the File '{theEntity['uuid']}' file info document" 
                                            f" from {target_index},"
                                            f" exception raised was {str(e)}.")
                    else:
                        raise ValueError(   f"Unable to find a Dataset or File with identifier {entity_id} whose"
                                            f" file info documents can be deleted from OpenSearch.")
                else:
                    # Since a File or a Dataset was not specified, delete all documents from
                    # the target index.
                    response += self.indexer.delete_fieldmatch_document(target_index)
                response += ' '
            return response
        else:
            raise ValueError(f"The index '{index}' is not recognized for delete_docs() operations.")

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
            logger.info(f"Start executing translate_upload() for {entity_id}")

            default_private_index = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['private']

            # Retrieve the upload entity details
            upload = self.call_entity_api(entity_id, 'entities')

            self.add_datasets_to_entity(upload)
            self.entity_keys_rename(upload)

            # Add additional calculated fields if any applies to Upload
            self.add_calculated_fields(upload)

            self.call_indexer(upload, reindex, json.dumps(upload), default_private_index)

            logger.info(f"Finished executing translate_upload() for {entity_id}")
        except Exception as e:
            logger.error(e)


    def translate_public_collection(self, entity_id, reindex=False):
        logger.info(f"Start executing translate_public_collection() for {entity_id}")

        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        try:
            collection = self.get_public_collection(entity_id)

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

            logger.info(f"Finished executing translate_public_collection() for {entity_id}")
        except requests.exceptions.RequestException as e:
            logger.exception(e)
            # Log the error and will need fix later and reindex, rather than sys.exit()
            logger.error(f"translate_public_collection() failed to get public collection of uuid: {entity_id} via entity-api")
        except Exception as e:
            logger.error(e)


    def translate_donor_tree(self, entity_id):
        try:
            logger.info(f"Start executing translate_donor_tree() for donor of uuid: {entity_id}")

            descendant_uuids = self.call_entity_api(entity_id, 'descendants', 'uuid')

            # Index the donor entity itself
            donor = self.call_entity_api(entity_id, 'entities')
            self.call_indexer(donor)

            # Index all the descendants of this donor
            with concurrent.futures.ThreadPoolExecutor() as executor:
                donor_descendants_list = [executor.submit(self.index_entity, uuid) for uuid in descendant_uuids]
                for f in concurrent.futures.as_completed(donor_descendants_list):
                    result = f.result()

            logger.info(f"Finished executing translate_donor_tree() for donor of uuid: {entity_id}")
        except Exception as e:
            logger.error(e)


    def index_entity(self, uuid):
        logger.info(f"Start executing index_entity() on uuid: {uuid}")

        entity_dict = self.call_entity_api(uuid, 'entities')
        self.call_indexer(entity_dict)

        logger.info(f"Finished executing index_entity() on uuid: {uuid}")


    # Used by individual PUT /reindex/<id> call
    def reindex_entity(self, uuid):
        logger.info(f"Start executing reindex_entity() on uuid: {uuid}")

        entity_dict = self.call_entity_api(uuid, 'entities')
        self.call_indexer(entity_dict, reindex=True)

        logger.info(f"Finished executing reindex_entity() on uuid: {uuid}")


    def init_transformers(self):
        logger.info("Start executing init_transformers()")

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

        logger.info("Finished executing init_transformers()")


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

    # Note: this entity dict input (if Dataset) has already removed ingest_metadata.files and
    # ingest_metadata.metadata sub fields with empty string values from previous call
    def call_indexer(self, entity, reindex=False, document=None, target_index=None):
        try:
            if document is None:
                try:
                    document = self.generate_doc(entity, 'json')
                except Exception as e:
                    logger.exception(e)
                    logger.error(f"Failed to execute generate_doc() on {entity['entity_type']} {entity['uuid']} during executing call_indexer()")

                    # Raise the exception so the caller can handle it properly
                    raise Exception(e)

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
                        try:
                            public_doc = self.generate_public_doc(entity)

                            if transformer is not None:
                                public_transformed = transformer.transform(json.loads(public_doc))
                                public_transformed_doc = json.dumps(public_transformed)
                                target_doc = public_transformed_doc
                            else:
                                target_doc = public_doc

                            self.indexer.index(entity['uuid'], target_doc, public_index, reindex)
                        except Exception:
                            msg = f"Exception encountered during executing generate_public_doc() inside call_indexer() for uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
                            # Log the full stack trace, prepend a line with our message
                            logger.exception(msg)

                    # add it to private
                    if transformer is not None:
                        private_transformed = transformer.transform(json.loads(document))
                        target_doc = json.dumps(private_transformed)
                    else:
                        target_doc = document

                    self.indexer.index(entity['uuid'], target_doc, private_index, reindex)
        except Exception as e:
            logger.exception(e)
            msg = f"Exception encountered during executing call_indexer() for uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
            # Log the full stack trace, prepend a line with our message
            logger.error(msg)


    # The added fields specified in `entity_properties_list` should not be added
    # to themselves as sub fields
    # The `except_properties_list` is a subset of entity_properties_list
    def exclude_added_top_level_properties(self, entity_data, except_properties_list = []):
        logger.info("Start executing exclude_added_top_level_properties()")

        if isinstance(entity_data, dict):
            for prop in entity_properties_list:
                if (prop in entity_data) and (prop not in except_properties_list):
                     entity_data.pop(prop)
        elif isinstance(entity_data, list):
            for prop in entity_properties_list:
                for item in entity_data:
                    if isinstance(item, dict) and (prop in item) and (prop not in except_properties_list):
                        item.pop(prop)
        else:
            logger.debug(f'The input entity_data type: {type(entity_data)}. Only dict and list are supported.')

        logger.info("Finished executing exclude_added_top_level_properties()")


    # Used for Upload and Collection index
    def add_datasets_to_entity(self, entity):
        logger.info("Start executing add_datasets_to_entity()")

        datasets = []
        if 'datasets' in entity:
            for dataset in entity['datasets']:
                # Retrieve the entity details
                try:
                    dataset = self.call_entity_api(dataset['uuid'], 'entities')
                except Exception as e:
                    logger.exception(e)
                    logger.error(f"Failed to retrieve dataset {dataset['uuid']} via entity-api during executing add_datasets_to_entity(), skip and continue to next one")
                    
                    # This can happen when the dataset is in neo4j but the actual uuid is not found in MySQL
                    # or somehting else is wrong with entity-api and it can't return the dataset info
                    # In this case, we'll skip over the current iteration, and continue with the next one
                    # Otherwise, null will be added to the  resuting datasets list and break portal-ui rendering - 5/3/2023 Zhou
                    continue
                
                try:
                    dataset_doc = self.generate_doc(dataset, 'dict')
                except Exception as e:
                    logger.exception(e)
                    logger.error(f"Failed to execute generate_doc() on dataset {dataset['uuid']} during executing add_datasets_to_entity(), skip and continue to next one")

                    # This can happen when the dataset itself is good but something failed to generate the doc
                    # E.g., one of the descendants of this dataset exists in neo4j but no record in uuid MySQL
                    # In this case, we'll skip over the current iteration, and continue with the next one
                    # Otherwise, no document is generated, null will be added to the resuting datasets list and break portal-ui rendering - 5/3/2023 Zhou
                    continue
                    
                self.exclude_added_top_level_properties(dataset_doc, except_properties_list = ['files', 'datasets'])
                datasets.append(dataset_doc)

        entity['datasets'] = datasets

        logger.info("Finished executing add_datasets_to_entity()")


    def entity_keys_rename(self, entity):
        logger.info("Start executing entity_keys_rename()")

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

        logger.info("Finished executing entity_keys_rename()")


    # These calculated fields are not stored in neo4j but will be generated
    # and added to the ES
    def add_calculated_fields(self, entity):
        logger.info("Start executing add_calculated_fields()")

        # Add index_version by parsing the VERSION file
        entity['index_version'] = self.index_version

        # Add display_subtype
        if entity['entity_type'] in entity_types_with_display_subtype:
            entity['display_subtype'] = self.generate_display_subtype(entity)

        logger.info("Finished executing add_calculated_fields()")


    # For Upload, Dataset, Donor and Sample objects:
    # add a calculated (not stored in Neo4j) field called `display_subtype` to
    # all Elasticsearch documents of the above types with the following rules:
    # Upload: Just make it "Data Upload" for all uploads
    # Donor: "Donor"
    # Sample: if sample_category == 'organ' the display name linked to the corresponding description of organ code
    # otherwise the display name linked to the value of the corresponding description of sample_category code
    # Dataset: the display names linked to the values in data_types as a comma separated list
    def generate_display_subtype(self, entity):
        logger.info("Start executing generate_display_subtype()")

        entity_type = entity['entity_type']
        display_subtype = '{unknown}'

        if entity_type == 'Upload':
            display_subtype = 'Data Upload'
        elif entity_type == 'Donor':
            display_subtype = 'Donor'
        elif entity_type == 'Sample':
            if 'sample_category' in entity:
                if entity['sample_category'].lower() == 'organ':
                    if 'organ' in entity:
                        display_subtype = get_type_description(entity['organ'], 'organ_types')
                    else:
                        logger.error(
                            f"Missing missing organ when sample_category is set of Sample with uuid: {entity['uuid']}")
                else:
                    display_subtype = get_type_description(entity['sample_category'], 'tissue_sample_types')
            else:
                logger.error(f"Missing sample_category of Sample with uuid: {entity['uuid']}")
        elif entity_type in ['Dataset', 'Publication']:
            if 'data_types' in entity:
                display_subtype = ','.join(entity['data_types'])
            else:
                logger.error(f"Missing data_types of Dataset with uuid: {entity['uuid']}")
        else:
            # Do nothing
            logger.error(
                f"Invalid entity_type: {entity_type}. Only generate display_subtype for Upload/Donor/Sample/Dataset")

        logger.info("Finished executing generate_display_subtype()")

        return display_subtype


    # Note: this entity dict input (if Dataset) has already handled ingest_metadata.files (with empty string or missing)
    # and ingest_metadata.metadata sub fields with empty string values from previous call
    def generate_doc(self, entity, return_type):
        try:
            logger.info(f"Start executing generate_doc() for {entity['entity_type']} of uuid: {entity['uuid']}")

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
                    # No need to call self.prepare_dataset() here because
                    # self.call_entity_api() already handled that
                    ancestor_dict = self.call_entity_api(ancestor_uuid, 'entities')
                    ancestors.append(ancestor_dict)

                # Find the Donor
                donor = None
                for a in ancestors:
                    if a['entity_type'] == 'Donor':
                        donor = copy.copy(a)
                        break

                descendant_ids = self.call_entity_api(entity_id, 'descendants', 'uuid')
                for descendant_uuid in descendant_ids:
                    descendant_dict = self.call_entity_api(descendant_uuid, 'entities')
                    descendants.append(descendant_dict)

                immediate_ancestor_ids = self.call_entity_api(entity_id, 'parents', 'uuid')
                for immediate_ancestor_uuid in immediate_ancestor_ids:
                    immediate_ancestor_dict = self.call_entity_api(immediate_ancestor_uuid, 'entities')
                    immediate_ancestors.append(immediate_ancestor_dict)

                immediate_descendant_ids = self.call_entity_api(entity_id, 'children', 'uuid')
                for immediate_descendant_uuid in immediate_descendant_ids:
                    immediate_descendant_dict = self.call_entity_api(immediate_descendant_uuid, 'entities')
                    immediate_descendants.append(immediate_descendant_dict)

                # Add new properties to entity
                entity['ancestors'] = ancestors
                entity['descendants'] = descendants

                entity['ancestor_ids'] = ancestor_ids
                entity['descendant_ids'] = descendant_ids

                entity['immediate_ancestors'] = immediate_ancestors
                entity['immediate_descendants'] = immediate_descendants

            # The `sample_category` is "organ" and the `organ` code is set at the same time
            if entity['entity_type'] in ['Sample', 'Dataset', 'Publication']:
                # Add new properties
                entity['donor'] = donor

                # entity['origin_samples'] is a list
                entity['origin_samples'] = []
                if ('sample_category' in entity) and (entity['sample_category'].lower() == 'organ') and ('organ' in entity) and (entity['organ'].strip() != ''):
                    entity['origin_samples'].append(copy.copy(entity))
                else:
                    for ancestor in ancestors:
                        if ('sample_category' in ancestor) and (ancestor['sample_category'].lower() == 'organ') and ('organ' in ancestor) and (ancestor['organ'].strip() != ''):
                            entity['origin_samples'].append(ancestor)

                # Remove those added fields specified in `entity_properties_list` from source_samples
                self.exclude_added_top_level_properties(entity['origin_samples'])
                
                # `source_samples` field is only avaiable to Dataset
                if entity['entity_type'] in ['Dataset', 'Publication']:
                    entity['source_samples'] = None
                    e = entity

                    while entity['source_samples'] is None:
                        parents = self.call_entity_api(e['uuid'], 'parents')

                        try:
                            if parents[0]['entity_type'] == 'Sample':
                                entity['source_samples'] = parents
                            e = parents[0]
                        except IndexError:
                            entity['source_samples'] = []
                    
                    # Remove those added fields specified in `entity_properties_list` from source_samples
                    self.exclude_added_top_level_properties(entity['source_samples'])

            self.entity_keys_rename(entity)

            # Is group_uuid always set?
            # In case if group_name not set
            if ('group_uuid' in entity) and ('group_name' not in entity):
                group_uuid = entity['group_uuid']

                # Get the globus groups info based on the groups json file in commons package
                auth_helper_instance = self.init_auth_helper()
                globus_groups_info = auth_helper_instance.get_globus_groups_info()
                groups_by_id_dict = globus_groups_info['by_id']
                group_dict = groups_by_id_dict[group_uuid]

                # Add new property
                entity['group_name'] = group_dict['displayname']

            # Rename for properties that are objects
            if entity.get('donor', None):
                self.entity_keys_rename(entity['donor'])

            if entity.get('origin_samples', None):
                for o in entity.get('origin_samples', None):
                    self.entity_keys_rename(o)
            if entity.get('source_samples', None):
                for s in entity.get('source_samples', None):
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

            logger.info(f"Finished executing generate_doc() for {entity['entity_type']} of uuid: {entity['uuid']}")

            return json.dumps(entity) if return_type == 'json' else entity
        except Exception as e:
            msg = "Exceptions during executing indexer.generate_doc()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            # Raise the exception so the caller can handle it properly
            raise Exception(e)


    def generate_public_doc(self, entity):
        logger.info(f"Start executing generate_public_doc() for {entity['entity_type']} of uuid: {entity['uuid']}")

        # Only Dataset has this 'next_revision_uuid' property
        property_key = 'next_revision_uuid'
        if (entity['entity_type'] in ['Dataset', 'Publication']) and (property_key in entity):
            next_revision_uuid = entity[property_key]
            
            # Can't reuse call_entity_api() here due to the response data type
            # Making a call against entity-api/entities/<next_revision_uuid>?property=status
            url = self.entity_api_url + "/entities/" + next_revision_uuid + "?property=status"
            response = requests.get(url, headers=self.request_headers, verify=False)

            if response.status_code != 200:
                logger.error(f"generate_public_doc() failed to get Dataset/Publication status of next_revision_uuid via entity-api for uuid: {next_revision_uuid}")
                
                # Bubble up the error message from entity-api instead of sys.exit(msg)
                # The caller will need to handle this exception
                response.raise_for_status()
                raise requests.exceptions.RequestException(response.text)

            # The call to entity-api returns string directly
            dataset_status = (response.text).lower()

            # Check the `next_revision_uuid` and if the dataset is not published,
            # pop the `next_revision_uuid` from this entity
            if dataset_status != self.DATASET_STATUS_PUBLISHED:
                logger.debug(f"Remove the {property_key} property from {entity['uuid']}")
                entity.pop(property_key)

        entity['descendants'] = list(filter(self.is_public, entity['descendants']))
        entity['immediate_descendants'] = list(filter(self.is_public, entity['immediate_descendants']))

        logger.info(f"Finished executing generate_public_doc() for {entity['entity_type']} of uuid: {entity['uuid']}")

        return json.dumps(entity)


    # The input `dataset_dict` can be a list if the entity-api returns a list, other times it's dict
    # Only applies to Dataset, no change to other entity types:
    # - Add the top-level 'files' field and set to empty list [] as default
    # - Set `ingest_metadata.files` to empty list [] when value is string (regardless empty or not) or the filed is missing
    # - Copy the actual files info list ['ingest_metadata']['files'] to the added top-level field
    # - Remove `ingest_metadata.metadata.*` sub fields when value is empty string
    def prepare_dataset(self, dataset_dict):
        logger.info("Start executing prepare_dataset()")

        # Add this top-level field for Dataset and set to empty list as default
        if (isinstance(dataset_dict, dict)) and ('entity_type' in dataset_dict) and (dataset_dict['entity_type'] in ['Dataset', 'Publication'] ):
            dataset_dict['files'] = []

            if 'ingest_metadata' in dataset_dict:
                if 'files' in dataset_dict['ingest_metadata']:
                    if isinstance(dataset_dict['ingest_metadata']['files'], list):
                        # Copy the actual files info list to the added top-level field
                        dataset_dict['files'] = dataset_dict['ingest_metadata']['files']
                    elif isinstance(dataset_dict['ingest_metadata']['files'], str):
                        # Set the original value to an emtpy list to avid mapping error
                        dataset_dict['ingest_metadata']['files'] = []
                        logger.info(f"Set ['ingest_metadata']['files'] to empty list [] due to string value {dataset_dict['ingest_metadata']['files']} found, for Dataset {dataset_dict['uuid']}")
                    else:
                        logger.error(f"Invalid data type of ['ingest_metadata']['files'], for Dataset {dataset_dict['uuid']}")
                else:
                    dataset_dict['ingest_metadata']['files'] = []
                    logger.info(f"Add missing field ['ingest_metadata']['files'] and set to empty list [], for Dataset {dataset_dict['uuid']}")

            # Remove any `ingest_metadata.metadata.*` sub fields if the value is empty string or just whitespace
            # to to avoid dynamic mapping conflict
            if ('ingest_metadata' in dataset_dict) and ('metadata' in dataset_dict['ingest_metadata']):
                for key in list(dataset_dict['ingest_metadata']['metadata']):
                    if isinstance(dataset_dict['ingest_metadata']['metadata'][key], str):
                        if not dataset_dict['ingest_metadata']['metadata'][key] or re.search(r'^\s+$', dataset_dict['ingest_metadata']['metadata'][key]):
                            del dataset_dict['ingest_metadata']['metadata'][key]
                            logger.info(f"Removed ['ingest_metadata']['metadata']['{key}'] due to empty string value, for Dataset {dataset_dict['uuid']}")
        
        logger.info("Finished executing prepare_dataset()")

        return dataset_dict

    # This method is supposed to only retrieve Dataset|Donor|Sample
    # The Collection and Upload are handled by separate calls
    # The returned data can either be an entity dict or a list of uuids (when `url_property` parameter is specified)
    def call_entity_api(self, entity_id, endpoint, url_property = None):
        logger.info(f"Start executing call_entity_api() on uuid: {entity_id}")

        url = self.entity_api_url + "/" + endpoint + "/" + entity_id
        if url_property:
            url += "?property=" + url_property

        response = requests.get(url, headers=self.request_headers, verify=False)

        if response.status_code != 200:
            msg = f"call_entity_api() failed to get entity of uuid {entity_id} via entity-api"

            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            logger.debug(f"======call_entity_api() status code from entity-api: {response.status_code}======")

            logger.debug("======call_entity_api() response text from entity-api======")
            logger.debug(response.text)

            # Add this uuid to the failed list
            self.failed_entity_api_calls.append(url)
            self.failed_entity_ids.append(entity_id)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        logger.info(f"Finished executing call_entity_api() on uuid: {entity_id}")

        # The resulting data can be an entity dict or a list (when `url_property` parameter is specified)
        # For Dataset, data manipulation is performed
        # If result is a list or not a Dataset dict, no change - 7/13/2022 Max & Zhou
        return self.prepare_dataset(response.json())


    def get_public_collection(self, entity_id):
        logger.info(f"Start executing get_public_collection() on uuid: {entity_id}")

        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        url = self.entity_api_url + "/collections/" + entity_id
        response = requests.get(url, headers=self.request_headers, verify=False)

        if response.status_code != 200:
            msg = f"get_public_collection() failed to get entity of uuid {entity_id} via entity-api"

            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            logger.debug("======get_public_collection() status code from entity-api======")
            logger.debug(response.status_code)

            logger.debug("======get_public_collection() response text from entity-api======")
            logger.debug(response.text)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        collection_dict = response.json()

        logger.info(f"Finished executing get_public_collection() on uuid: {entity_id}")

        return collection_dict


    def delete_and_recreate_indices(self):
        try:
            logger.info("Start executing delete_and_recreate_indices()")

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

            logger.info("Finished executing delete_and_recreate_indices()")
        except Exception:
            msg = "Exception encountered during executing delete_and_recreate_indices()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)



# Running full reindex script in command line
# This approach is different from the live /reindex-all PUT call
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
    
    # Skip the uuids comparision step that is only needed for live /reindex-all PUT call
    translator.skip_comparision = True

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
    if not auth_helper.has_data_admin_privs(token):
        msg = "The given token doesn't belong to the HuBMAP-Data-Admin group, access not granted"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    start = time.time()
    logger.info("############# Full index via script started #############")

    translator.delete_and_recreate_indices()
    translator.translate_all()

    # Show the failed entity-api calls and the uuids
    if translator.failed_entity_api_calls:
        logger.info(f"{len(translator.failed_entity_api_calls)} entity-api calls failed")
        print(*translator.failed_entity_api_calls, sep = "\n")
 
    if translator.failed_entity_ids:
        logger.info(f"{len(translator.failed_entity_ids)} entity ids failed")
        print(*translator.failed_entity_ids, sep = "\n")

    end = time.time()
    logger.info(f"############# Full index via script completed. Total time used: {end - start} seconds. #############")
