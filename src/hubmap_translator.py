import concurrent.futures
import copy
import importlib
import requests
import json
import logging
import os
import re
import sys
import time
from yaml import safe_load, YAMLError
from http.client import HTTPException
from enum import Enum
from types import MappingProxyType

# For reusing the app.cfg configuration when running indexer_base.py as script
from flask import Flask, Response

# Local modules
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
    'immediate_ancestors',
    'immediate_descendants',
    'immediate_ancestor_ids',
    'immediate_descendant_ids'
]

# Entity types that will have `display_subtype` generated at index time
entity_types_with_display_subtype = ['Upload', 'Donor', 'Sample', 'Dataset', 'Publication']

# A list of fields to be excluded from the contents of top-level fields of
# Collection and Upload entities when indexing these entities into ElasticSearch documents.
NESTED_EXCLUDED_ES_FIELDS_FOR_COLLECTIONS_AND_UPLOADS = ['ingest_metadata','metadata','files']

# Define an enumeration to classify the elements of a top-level property listed in entity_properties_list as
# either retained to write into the ElasticSearch document, or only for inclusion for calculations but
# not to be written to the ES doc.
class PropertyRetentionEnum(Enum):
    # Property from entity object should be written to the ElasticSearch document (and may be used for calculations.)
    ES_DOC = 'es_doc'
    # Property from entity object is only retained while doing calculations, and must be removed before
    # writing the entity to an ElasticSearch document.
    CALC_ONLY = 'calc_only'

# For ElasticSearch documents being written to the 'entities' indices, these are the
# fields to retain within fields which entity-api added via triggers e.g. 'descendants' and 'ancestors'
INDEX_GROUP_ENTITIES_DOC_FIELDS = {
    'dataset_type': PropertyRetentionEnum.ES_DOC
    , 'rui_location': PropertyRetentionEnum.ES_DOC
    , 'uuid': PropertyRetentionEnum.ES_DOC
    , 'hubmap_id': PropertyRetentionEnum.CALC_ONLY
    , 'entity_type': PropertyRetentionEnum.CALC_ONLY
    , 'group_uuid': PropertyRetentionEnum.CALC_ONLY
    , 'group_name': PropertyRetentionEnum.CALC_ONLY
    , 'last_modified_timestamp': PropertyRetentionEnum.CALC_ONLY
    , 'created_by_user_displayname': PropertyRetentionEnum.CALC_ONLY
    , 'thumbnail_file': PropertyRetentionEnum.CALC_ONLY
    , 'sample_category': PropertyRetentionEnum.CALC_ONLY # Needed to fill origin_samples
    , 'organ': PropertyRetentionEnum.CALC_ONLY # Needed to fill origin_samples
    , 'data_access_level': PropertyRetentionEnum.CALC_ONLY # Needed for is_public() calculations for Sample
    , 'status': PropertyRetentionEnum.CALC_ONLY # Needed for is_public() calculations for Dataset & Publication
}

# For ElasticSearch documents being written to the 'portal' indices, these are the
# fields to retain within fields which entity-api added via triggers e.g. 'descendants' and 'ancestors'
INDEX_GROUP_PORTAL_DOC_FIELDS = {
    'uuid': PropertyRetentionEnum.ES_DOC
    , 'entity_type': PropertyRetentionEnum.ES_DOC
    , 'data_access_level': PropertyRetentionEnum.CALC_ONLY  # Needed for is_public() calculations for Sample
    , 'status': PropertyRetentionEnum.CALC_ONLY  # Needed for is_public() calculations for Dataset & Publication
}

class Translator(TranslatorInterface):
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    DATASET_STATUS_PUBLISHED = 'published'
    # Constants to build endpoint URLs for Ontology API
    ONTOLOGY_API_ORGAN_TYPES_ENDPOINT = '/organs?application_context=HUBMAP'
    DEFAULT_INDEX_WITHOUT_PREFIX = ''
    INDICES = {}
    TRANSFORMERS = {}
    DEFAULT_ENTITY_API_URL = ''
    indexer = None
    skip_comparision = False
    failed_entity_api_calls = []
    failed_entity_ids = []

    def __init__(self, indices, app_client_id, app_client_secret, token, ontology_api_base_url:str=None):
        try:
            self.ingest_api_soft_assay_url = indices['ingest_api_soft_assay_url'].strip('/')
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
            self._ontology_api_base_url = ontology_api_base_url

            if not indices['entity_api_prov_schema_raw_url']:
                raise Exception(f"Unable read the URL for access to Entity API's provenance_schema.yaml using the translator's"
                                f" indices['entity_api_prov_schema_raw_url']={indices['entity_api_prov_schema_raw_url']}.")

            # Commented out by Zhou to avoid 409 conflicts - 7/20/2024
            # self.es_retry_on_conflict_param_value = indices['es_retry_on_conflict_param_value']

            self.indexer = Indexer(self.indices, self.DEFAULT_INDEX_WITHOUT_PREFIX)

            # Keep a dictionary of each ElasticSearch index in an index group which may be
            # looked up for the re-indexing process.
            self.index_group_es_indices = {
                'entities': {
                    'public': f"{self.INDICES['indices']['entities']['public']}"
                    , 'private': f"{self.INDICES['indices']['entities']['private']}"
                }
                ,'portal': {
                    'public': f"{self.INDICES['indices']['portal']['public']}"
                    , 'private': f"{self.INDICES['indices']['portal']['private']}"
                }
            }

            logger.debug("=========== INDICES config ===========")
            logger.debug(self.INDICES)
        except Exception as e:
            logger.error(f"Error loading configuration. e={str(e)}")
            raise ValueError("Invalid indices config. See logs")

        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token

        try:
            self.request_headers = self.create_request_headers_for_auth(token)
            self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')
            # Add index_version by parsing the VERSION file
            self.index_version = ((Path(__file__).absolute().parent.parent / 'VERSION').read_text()).strip()
            self.transformation_resources = {'ingest_api_soft_assay_url': self.ingest_api_soft_assay_url,
                                             'organ_map': self.get_organ_types(),
                                             'descendants_url': f'{self.entity_api_url}/descendants',
                                             'token': token,}



            # # Preload all the transformers
            self.init_transformers()

            # Preload the list of fields per entity type to be excluded from public index documents, from the
            # source also used by entity-api
            self.load_public_doc_exclusion_dict(entity_api_prov_schema_raw_url=indices['entity_api_prov_schema_raw_url'])
            # The entity types covered by the public doc exclusion dictionary may also occur under
            # nested fields which the loaded Entity API YAML does not know about. Supplement the
            # dictionary so the fields are excluded throughout the document in the public index.
            self.supplement_public_doc_exclusion_dict()
        except Exception as e:
            msg = 'Error configuring translator during initialization'
            logger.error(f"{msg}, e={str(e)}")
            raise ValueError(f"{msg}. See logs")

    def log_configuration(self, log_level:int=logger.getEffectiveLevel()):
        logger.log( level=log_level
                    , msg=f"\tingest_api_soft_assay_url={self.ingest_api_soft_assay_url}")
        logger.log(level=log_level
                   , msg=f"\tindices={self.indices}")
        logger.log(level=log_level
                   , msg=f"\tself_managed_indices={self.self_managed_indices}")
        logger.log(level=log_level
                   , msg=f"\tDEFAULT_INDEX_WITHOUT_PREFIX={self.DEFAULT_INDEX_WITHOUT_PREFIX}")
        logger.log(level=log_level
                   , msg=f"\tINDICES={self.INDICES}")
        logger.log(level=log_level
                   , msg=f"\tDEFAULT_ENTITY_API_URL={self.DEFAULT_ENTITY_API_URL}")
        logger.log(level=log_level
                   , msg=f"\t_ontology_api_base_url={self._ontology_api_base_url}")
        logger.log(level=log_level
                   , msg=f"\tindexer={self.indexer}")
        logger.log(level=log_level
                   , msg=f"\tindex_group_es_indices={self.index_group_es_indices}")
        logger.log(level=log_level
                   , msg=f"\tapp_client_id={self.app_client_id}")
        logger.log(level=log_level
                   , msg=f"\tapp_client_secret will not be logged")
        logger.log(level=log_level
                   , msg=f"\ttoken={self.token}")
        logger.log(level=log_level
                   , msg=f"\trequest_headers={self.request_headers}")
        logger.log(level=log_level
                   , msg=f"\tentity_api_url={self.entity_api_url}")
        logger.log(level=log_level
                   , msg=f"\tindex_version={self.index_version}")
        logger.log(level=log_level
                   , msg=f"\ttransformation_resources={self.transformation_resources}")
        logger.log(level=log_level
                    , msg=f"\tTRANSFORMERS={self.TRANSFORMERS}")

    # Used by full reindex via script and live reindex-all call
    def translate_all(self):
        with app.app_context():
            try:
                logger.info("Start executing translate_all()")

                start = time.time()

                donor_uuids_list = get_uuids_by_entity_type("donor", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                upload_uuids_list = get_uuids_by_entity_type("upload", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                collection_uuids_list = get_uuids_by_entity_type("collection", self.request_headers, self.DEFAULT_ENTITY_API_URL)

                # Only need this comparision for the live /rindex-all PUT call
                if not self.skip_comparision:
                    # Make calls to entity-api to get a list of uuids for rest of entity types
                    sample_uuids_list = get_uuids_by_entity_type("sample", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                    dataset_uuids_list = get_uuids_by_entity_type("dataset", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                    
                    # Merge into a big list that with no duplicates
                    all_entities_uuids = set(donor_uuids_list + sample_uuids_list + dataset_uuids_list + upload_uuids_list + collection_uuids_list)

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
                    collection_futures_list = [executor.submit(self.translate_collection, uuid, reindex=True) for uuid in collection_uuids_list]
                    upload_futures_list = [executor.submit(self.translate_upload, uuid, reindex=True) for uuid in upload_uuids_list]

                    # Append the above lists into one
                    futures_list = collection_futures_list + upload_futures_list

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

    # Used by full reindex scripts only.
    # Assumes the index named indices are already created and empty.
    # Require Data Admin privileges to execute.
    def translate_full(self):
        auth_helper_instance = self.init_auth_helper()
        if not auth_helper_instance.has_data_admin_privs(self.token):
            raise Exception('Data admin privileges are required to fill specific indices.')

        with ((app.app_context())):
            try:
                start_time = time.time()
                logger.info(f"############# Start executing translate_full() at"
                            f" {time.strftime('%H:%M:%S', time.localtime(start_time))}"
                            f" #############")

                donor_uuids_list = get_uuids_by_entity_type("donor", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                if len(donor_uuids_list) == 2 and isinstance(donor_uuids_list[1], int):
                    # Ask SenNet if I can change this! KBKBKB @TODO
                    raise Exception(f"Fetching UUIDs for donor returned an HTTP {donor_uuids_list[1]} error.")
                upload_uuids_list = get_uuids_by_entity_type("upload", self.request_headers,
                                                             self.DEFAULT_ENTITY_API_URL)
                if len(upload_uuids_list) == 2 and isinstance(upload_uuids_list[1], int):
                    # Ask SenNet if I can change this! KBKBKB @TODO
                    raise Exception(f"Fetching UUIDs for uploads returned an HTTP {upload_uuids_list[1]} error.")
                collection_uuids_list = get_uuids_by_entity_type("collection", self.request_headers,
                                                                 self.DEFAULT_ENTITY_API_URL)
                if len(collection_uuids_list) == 2 and isinstance(collection_uuids_list[1], int):
                    # Ask SenNet if I can change this! KBKBKB @TODO
                    raise Exception(f"Fetching UUIDs for collections returned an HTTP {collection_uuids_list[1]} error.")

                logger.info(    f"Indexing {len(donor_uuids_list)} Donors,"
                                f" {len(upload_uuids_list)} Uploads,"
                                f" and {len(collection_uuids_list)} Collections.")

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    # The default number of threads in the ThreadPoolExecutor is calculated as:
                    # From 3.8 onwards default value is min(32, os.cpu_count() + 4)
                    # Where the number of CPUs is determined by Python and will take hyperthreading into account
                    logger.info(f"The number of worker threads being used by default: {executor._max_workers}")

                    # Submit tasks to the thread pool
                    collection_futures_list = [executor.submit(self.translate_collection, uuid, reindex=True) for uuid
                                               in collection_uuids_list]
                    upload_futures_list = [executor.submit(self.translate_upload, uuid, reindex=True) for uuid in
                                           upload_uuids_list]

                    # Append the above lists into one
                    futures_list = collection_futures_list + upload_futures_list

                    # The target function runs the task logs more details when f.result() gets executed
                    for f in concurrent.futures.as_completed(futures_list):
                        result = f.result()

                # Index the donor tree in a regular for loop, not the concurrent mode
                # However, the descendants of a given donor will be indexed concurrently
                for uuid in donor_uuids_list:
                    self.translate_donor_tree(uuid)

                end_time = time.time()
                logger.info(f"############# Finished executing translate_full() at"
                            f" {time.strftime('%H:%M:%S', time.localtime(end_time))}."
                            f" #############")
                elapsed_seconds = end_time - start_time
                logger.info(f"############# Executing translate_full() took"
                            f" {time.strftime('%H:%M:%S', time.gmtime(elapsed_seconds))}."
                            f" #############")
            except Exception as e:
                logger.exception(e)

    # ONLY used by collections-only reindex via script - added by Zhou 7/19/2023
    def translate_all_collections(self):
        with app.app_context():
            try:
                logger.info("Start executing translate_all_collections()")

                start = time.time()
                collection_uuids_list = get_uuids_by_entity_type("collection", self.request_headers, self.DEFAULT_ENTITY_API_URL)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    # The default number of threads in the ThreadPoolExecutor is calculated as: 
                    # From 3.8 onwards default value is min(32, os.cpu_count() + 4)
                    # Where the number of CPUs is determined by Python and will take hyperthreading into account
                    logger.info(f"The number of worker threads being used by default: {executor._max_workers}")

                    # Submit tasks to the thread pool
                    collection_futures_list = [executor.submit(self.translate_collection, uuid, reindex=True) for uuid in collection_uuids_list]

                    # The target function runs the task logs more details when f.result() gets executed
                    for f in concurrent.futures.as_completed(collection_uuids_list):
                        result = f.result()

                end = time.time()

                logger.info(f"Finished executing translate_all_collections(). Total time used: {end - start} seconds.")
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



    # # Commented out by Zhou to avoid 409 conflicts - 7/20/2024
    # def _relationships_changed_since_indexed( self, neo4j_ancestor_ids:list[str], neo4j_descendant_ids:list[str],
    #                                             existing_oss_doc:json):
    #     # Start with the safe assumption that relationships have changed, and
    #     # only toggle if verified unchanged below
    #     relationships_changed = True

    #     # Get the ancestors and descendants of this entity as they exist in OpenSearch.
    #     oss_ancestor_ids = []
    #     if existing_oss_doc and 'fields' in existing_oss_doc and 'ancestor_ids' in existing_oss_doc['fields']:
    #         oss_ancestor_ids = existing_oss_doc['fields']['ancestor_ids']
    #     oss_descendant_ids = []
    #     if existing_oss_doc and 'fields' in existing_oss_doc and 'descendant_ids' in existing_oss_doc['fields']:
    #         oss_descendant_ids = existing_oss_doc['fields']['descendant_ids']

    #     # If the ancestor list and descendant list on the OpenSearch document for this entity are
    #     # not both exactly the same set of IDs as in Neo4j, relationships have changed and this
    #     # entity must be re-indexed rather than just updating existing documents for associated entities.
    #     #
    #     # These lists are implicitly sets, as they do not have duplicates and order does not mean anything.
    #     # Leave algorithmic efficiency to Python's implementation of sets.
    #     neo4j_descendant_id_set = frozenset(neo4j_descendant_ids)
    #     oss_descendant_id_set = frozenset(oss_descendant_ids)

    #     if not neo4j_descendant_id_set.symmetric_difference(oss_descendant_id_set):
    #         # Since the descendants are unchanged, check the ancestors to decide if re-indexing must be done.
    #         neo4j_ancestor_id_set = frozenset(neo4j_ancestor_ids)
    #         oss_ancestor_id_set = frozenset(oss_ancestor_ids)

    #         if not neo4j_ancestor_id_set.symmetric_difference(oss_ancestor_id_set):
    #             relationships_changed = False

    #     return relationships_changed


    # # Commented out by Zhou to avoid 409 conflicts - 7/20/2024
    # def _get_existing_entity_relationships(self, entity_uuid:str, es_url:str, es_index:str):
    #     # Form a simple match query, and retrieve an existing OpenSearch document for entity_id, if it exists.
    #     # N.B. This query does not pass through the AWS Gateway, so we will not have to retrieve the
    #     #      result from an AWS S3 Bucket.  If it is larger than 10Mb, we will get it directly.
    #     QDSL_SEARCH_ENDPOINT_MATCH_UUID_PATTERN =(
    #         '{ ' + \
    #         '"query": {  "bool": { "filter": [ {"terms": {"uuid": ["<TARGET_SEARCH_UUID>"]}} ] } }' + \
    #         ', "fields": ["ancestor_ids", "descendant_ids"] ,"_source": false' + \
    #         ' }')

    #     qdsl_search_query_payload_string = QDSL_SEARCH_ENDPOINT_MATCH_UUID_PATTERN.replace('<TARGET_SEARCH_UUID>'
    #                                                                                        , entity_uuid)
    #     json_query_dict = json.loads(qdsl_search_query_payload_string)
    #     opensearch_response = execute_opensearch_query(query_against='_search'
    #                                                    , request=None
    #                                                    , index=es_index
    #                                                    , es_url=es_url
    #                                                    , query=json_query_dict
    #                                                    , request_params={'filter_path': 'hits.hits'})

    #     # Verify the expected response was returned.  If no document was returned, proceed with a re-indexing.
    #     # If exactly one document is returned, distill it down to JSON used to update document fields.
    #     if opensearch_response.status_code != 200:
    #         logger.error(f"Unable to return ['hits']['hits'] content of opensearch_response for"
    #                      f" es_url={es_url}, with"
    #                      f" status_code={opensearch_response.status_code}.")
    #         raise Exception(f"OpenSearch query return a status code of '{opensearch_response.status_code}'."
    #                         f" See logs.")

    #     resp_json = opensearch_response.json()

    #     if not resp_json or \
    #             'hits' not in resp_json or \
    #             'hits' not in resp_json['hits'] or \
    #             len(resp_json['hits']['hits']) == 0:
    #         # If OpenSearch does not have an existing document for this entity, drop down to reindexing.
    #         # Anything else Falsy JSON could be an unexpected result for an existing entity, but fall back to
    #         # reindexing under those circumstances, too.
    #         pass
    #     elif len(resp_json['hits']['hits']) != 1:
    #         # From the index populated with everything, raise an exception if exactly one document for the
    #         # current entity is not what is returned.
    #         logger.error(f"Found {len(resp_json['hits']['hits'])} documents instead"
    #                      f" of a single document searching resp_json['hits']['hits'] from opensearch_response with"
    #                      f" es_url={es_url},"
    #                      f" json_query_dict={json_query_dict}.")
    #         raise Exception(f"Unexpected response to OpenSearch query for a single entity document."
    #                         f" See logs.")
    #     elif 'fields' not in resp_json['hits']['hits'][0]:
    #         # The QDSL query may return exactly one resp_json['hits']['hits'] if called for an
    #         # entity which has a document but not the fields searched for e.g. a Donor being
    #         # created with no ancestors or descendants yet. Return empty
    #         # JSON rather than indicating this is an error.
    #         return {}
    #     else:
    #         # Strip away whatever remains of OpenSearch artifacts, such as _source, to get to the
    #         # exact JSON of this entity's existing, so that can become a part of the other documents which
    #         # retain a snapshot of this entity, such as this entity's ancestors, this entity's descendants,
    #         # Collection entity's containing this entity, etc.
    #         # N.B. Many such artifacts should have already been stripped through usage of the filter_path.
    #         return resp_json['hits']['hits'][0]



    # # Commented out by Zhou to avoid 409 conflicts - 7/20/2024
    # def _directly_modify_related_entities(  self, es_url:str, es_index:str, entity_id:str
    #                                         , neo4j_ancestor_ids:list[str], neo4j_descendant_ids:list[str]
    #                                         , neo4j_collection_ids:list[str], neo4j_upload_ids:list[str]):
    #     # Directly update the OpenSearch documents for each associated entity with a current snapshot of
    #     # this entity, since relationships graph of this entity is unchanged in Neo4j since the
    #     # last time this entity was indexed.
    #     #
    #     # Given updated JSON for the OpenSearch document of this entity, replace the snapshot of
    #     # this entity's JSON in every OpenSearch document it appears in.
    #     # Each document for an identifier in the 'ancestors' list of this entity will need to have one
    #     # member of its 'descendants' list updated. Similarly, each OpenSearch document for a descendant
    #     # entity will need one member of its 'ancestors' list updated.
    #     # 'immediate_ancestors' will be updated for every entity which is an 'immediate_descendant'.
    #     # 'immediate_descendants' will be updated for every entity which is an 'immediate_ancestor'.

    #     # Retrieve the entity details
    #     # This returned entity dict (if Dataset) has removed ingest_metadata.files and
    #     # ingest_metadata.metadata sub fields with empty string values when call_entity_api() gets called
    #     revised_entity_doc_dict = self.call_entity_api(entity_id=entity_id
    #                                                    , endpoint_base='documents')

    #     painless_query = f'for (prop in <TARGET_DOC_ELEMENT_LIST>)' \
    #                      f' {{if (ctx._source.containsKey(prop))' \
    #                      f'  {{for (int i = 0; i < ctx._source[prop].length; ++i)' \
    #                      f'   {{if (ctx._source[prop][i][\'uuid\'] == params.modified_entity_uuid)' \
    #                      f'    {{ctx._source[prop][i] = params.revised_related_entity}} }} }} }}'
    #     QDSL_UPDATE_ENDPOINT_WITH_ID_PARAM = \
    #         f'{{\"script\": {{' \
    #         f'  \"lang\": \"painless\",' \
    #         f'  \"source\": \"{painless_query}\",' \
    #         f'  \"params\": {{' \
    #         f'   \"modified_entity_uuid\": \"<TARGET_MODIFIED_ENTITY_UUID>\",' \
    #         f'   \"revised_related_entity\": <THIS_REVISED_ENTITY>' \
    #         f'  }}' \
    #         f' }} }}'
    #     # Eliminate taking advantage of our knowledge that an ancestor only needs its descendants lists
    #     # updated and a descendant only needs its ancestor lists updated.  Instead, focus upon consolidating
    #     # updates into a single query for the related entity's document to avoid HTTP 409 Conflict
    #     # problems if too many queries post for a single document.
    #     related_entity_target_elements = [  'immediate_descendants'
    #                                         , 'descendants'
    #                                         , 'immediate_ancestors'
    #                                         , 'ancestors'
    #                                         , 'source_samples'
    #                                         , 'origin_samples'
    #                                         , 'datasets']

    #     related_entity_ids = neo4j_ancestor_ids + neo4j_descendant_ids + neo4j_collection_ids + neo4j_upload_ids

    #     for related_entity_id in related_entity_ids:
    #         qdsl_update_payload_string = QDSL_UPDATE_ENDPOINT_WITH_ID_PARAM \
    #             .replace('<TARGET_MODIFIED_ENTITY_UUID>', entity_id) \
    #             .replace('<TARGET_DOC_ELEMENT_LIST>', str(related_entity_target_elements)) \
    #             .replace('<THIS_REVISED_ENTITY>', json.dumps(revised_entity_doc_dict))
    #         json_query_dict = json.loads(qdsl_update_payload_string)

    #         # Try to avoid 409
    #         query_params = {
    #             'retry_on_conflict': str(self.es_retry_on_conflict_param_value),
    #             'refresh': 'true'
    #         }

    #         opensearch_response = execute_opensearch_query(query_against=f"_update/{related_entity_id}"
    #                                                        , request=None
    #                                                        , index=es_index
    #                                                        , es_url=es_url
    #                                                        , query=json_query_dict
    #                                                        , request_params=query_params)
    #         # Expect an HTTP 200 on a successful update, and an HTTP 404 if es_index does not
    #         # contain a document for related_entity_id.  Other response codes are errors.
    #         if opensearch_response.status_code not in [200, 404]:
    #             logger.error(f"Unable to directly update document of {related_entity_id} with using the latest version of {entity_id} in"
    #                          f" related_entity_target_elements={related_entity_target_elements},"
    #                          f" endpoint '{es_index}/_update/{related_entity_id}'"
    #                          f" Got status_code={opensearch_response.status_code}.")

    #             if opensearch_response.text:
    #                 logger.error(f"OpenSearch message for {opensearch_response.status_code} code:"
    #                              f" '{opensearch_response.text}'.")
    #             raise Exception(f"OpenSearch query returned a status code of "
    #                             f" '{opensearch_response.status_code}'. See logs.")
    #         elif opensearch_response.status_code == 404:
    #             logger.info(f"Call to QDSL _update got HTTP response code"
    #                         f" {opensearch_response.status_code}, which is ignored because it"
    #                         f" should indicate"
    #                         f" related_entity_target_elements={related_entity_target_elements}"
    #                         f" is not in es_index={es_index}.")

    def _exec_reindex_entity_to_index_group_by_id(self, entity_id: str(32), index_groups: list):
        logger.info(f"Start executing _exec_reindex_entity_to_index_group_by_id() on"
                    f" entity_id: {entity_id}, index_groups: {str(index_groups)}")

        entity = self.call_entity_api(entity_id=entity_id
                                      , endpoint_base='documents')
        if entity['entity_type'] in ['Collection','Epicollection']:
            self.translate_collection(entity_id=entity_id, reindex=True)
        elif entity['entity_type'] == 'Upload':
            self.translate_upload(entity_id=entity_id, reindex=True)
        else:
            for index_group in index_groups:
                self._transform_and_write_entity_to_index_group(entity=entity
                                                                , index_group=index_group)

        logger.info(f"Finished executing _exec_reindex_entity_to_index_group_by_id()")


    def _transform_and_write_entity_to_index_group(self, entity:dict, index_group:str):
        logger.info(f"Start executing direct '{index_group}' updates for"
                    f" entity['uuid']={entity['uuid']},"
                    f" entity['entity_type']={entity['entity_type']}")

        try:
            # The entity dictionary will be changed by the document generation methods, and
            # _generate_public_doc() needs _generate_doc() to make changes first. So make
            # a copy of the entity for these methods, leaving the original argument intact
            doc_entity=copy.deepcopy(entity)
            private_doc = self._generate_doc(   entity=doc_entity
                                                , return_type='json'
                                                , index_group=index_group)
            if self.is_public(entity):
                public_doc = self._generate_public_doc( entity=doc_entity
                                                        , index_group=index_group)
            del doc_entity
        except Exception as e:
            msg = f"Exception document generation" \
                  f" for uuid: {entity['uuid']}, entity_type: {entity['entity_type']}" \
                  f" for '{index_group}' reindex caused \'{str(e)}\'"
            # Log the full stack trace, prepend a line with our message. But continue on
            # rather than raise the Exception.
            logger.exception(msg)

        if 'private_doc' not in locals() or  private_doc is None:
            logger.error(   f"For {entity['entity_type']} {entity['uuid']},"
                            f" failed to generate document for consortium indices.")

        docs_to_write_dict = {
            self.index_group_es_indices[index_group]['private']: None,
            self.index_group_es_indices[index_group]['public']: None
        }
        # Check to see if the index_group has a transformer, default to None if not found
        transformer = self.TRANSFORMERS.get(index_group, None)
        if transformer is None:
            logger.info(f"Unable to find '{index_group}' transformer, indexing documents untransformed.")
            docs_to_write_dict[self.index_group_es_indices[index_group]['private']] = private_doc
            if 'public_doc' in locals() and public_doc is not None:
                docs_to_write_dict[self.index_group_es_indices[index_group]['public']] = public_doc
        else:
            private_transformed = transformer.transform(json.loads(private_doc),
                                                        self.transformation_resources)
            docs_to_write_dict[self.index_group_es_indices[index_group]['private']] = json.dumps(private_transformed)
            if 'public_doc' in locals() and public_doc is not None:
                public_transformed = transformer.transform(json.loads(public_doc),
                                                           self.transformation_resources)
                docs_to_write_dict[self.index_group_es_indices[index_group]['public']] = json.dumps(public_transformed)

        for index_name in docs_to_write_dict.keys():
            if docs_to_write_dict[index_name] is None:
                continue
            self.indexer.index(entity_id=entity['uuid']
                               , document=docs_to_write_dict[index_name]
                               , index_name=index_name
                               , reindex=True)
            logger.info(f"Finished executing indexer.index() during direct '{index_group}' reindexing with" \
                        f" entity['uuid']={entity['uuid']}," \
                        f" entity['entity_type']={entity['entity_type']}," \
                        f" index_name={index_name}.")

        logger.info(f"Finished direct '{index_group}' updates for"
                    f" entity['uuid']={entity['uuid']},"
                    f" entity['entity_type']={entity['entity_type']}")

    # Used by individual live reindex call
    def translate(self, entity_id):
        try:
            # Retrieve the entity details
            # This returned entity dict (if Dataset) has removed ingest_metadata.files and
            # ingest_metadata.metadata sub fields with empty string values when call_entity_api() gets called
            entity = self.call_entity_api(entity_id=entity_id, endpoint_base='documents')

            logger.info(f"Start executing translate() on {entity['entity_type']} of uuid: {entity_id}")

            if entity['entity_type'] in ['Collection', 'Epicollection']:
                # Expect entity-api to stop update of Collections which should not be modified e.g. those which
                # have a DOI.  But entity-api may still request such Collections be indexed, particularly right
                # after the Collection becomes visible to the public.
                try:
                    self.translate_collection(entity_id ,reindex=True)
                except Exception as e:
                    logger.error(f"Unable to index {entity['entity_type']} due to e={str(e)}")

            elif entity['entity_type'] == 'Upload':
                self.translate_upload(entity_id, reindex=True)
            else:
                # BEGIN - Below block is the original implementation prior to the direct document update
                # against Elasticsearch. Added back by Zhou to avoid 409 conflicts - 7/20/2024

                previous_revision_ids = []
                next_revision_ids = []

                # Get the ancestors and descendants of this entity as they exist in Neo4j
                neo4j_ancestor_ids = self.call_entity_api(  entity_id=entity_id
                                                            , endpoint_base='ancestors'
                                                            , endpoint_suffix=None
                                                            , url_property='uuid')
                neo4j_descendant_ids = self.call_entity_api(    entity_id=entity_id
                                                                , endpoint_base='descendants'
                                                                , endpoint_suffix=None
                                                                , url_property='uuid')

                # Only Dataset/Publication entities may have previous/next revisions
                if entity['entity_type'] in ['Dataset', 'Publication']:
                    previous_revision_ids = self.call_entity_api(entity_id=entity_id
                                                                , endpoint_base='previous_revisions'
                                                                , endpoint_suffix=None
                                                                , url_property='uuid')
                    next_revision_ids = self.call_entity_api(entity_id=entity_id
                                                            , endpoint_base='next_revisions'
                                                            , endpoint_suffix=None
                                                            , url_property='uuid')

                # If working with a Dataset or Publication, it may be copied into ElasticSearch documents for
                # Collections and Uploads, so identify any of those which must be reindexed.
                neo4j_collection_ids = []
                neo4j_upload_ids = []
                if entity['entity_type'] in ['Dataset', 'Publication']:
                    neo4j_collection_ids = self.call_entity_api(entity_id=entity_id
                                                                , endpoint_base='entities'
                                                                , endpoint_suffix='collections'
                                                                , url_property='uuid')
                    neo4j_upload_ids = self.call_entity_api(entity_id=entity_id
                                                            , endpoint_base='entities'
                                                            , endpoint_suffix='uploads'
                                                            , url_property='uuid')

                # Reindex the entity itself first before dealing with other documents for related entities.
                self._call_indexer(entity=entity
                                   , delete_existing_doc_first=True)

                # All unique entity ids in the path excluding the entity itself
                target_ids = set(neo4j_ancestor_ids + neo4j_descendant_ids + \
                                 previous_revision_ids + next_revision_ids + \
                                 neo4j_collection_ids + neo4j_upload_ids)

                # Reindex the rest of the entities in the list
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures_list = [executor.submit(self._exec_reindex_entity_to_index_group_by_id, related_entity_uuid, ['entities','portal']) for related_entity_uuid in target_ids]
                    for f in concurrent.futures.as_completed(futures_list):
                        result = f.result()
                
                # END - Above block is the original implementation prior to the direct document update
                # against Elasticsearch. Added back by Zhou to avoid 409 conflicts - 7/20/2024



                # # Commented out by Zhou to avoid 409 conflicts - 7/20/2024
                # # For newly created entities and entities whose relationships in Neo4j have changed since the
                # # entity was indexed into OpenSearch, use "reindex" code to bring the OpenSearch document
                # # up-to-date for the entity and all the entities it relates to.
                # #
                # # For entities previously indexed into OpenSearch whose relationships in Neo4j have not changed,
                # # just index the document for the entity. Then update fields belong to related entities which
                # # refer to the entity i.e. the 'ancestors' list of this entity's 'descendants', the 'descendants'
                # # list of this entity's 'ancestors', etc.
                # # N.B. As of Spring '24, this shortcut can only be done for the 'entities' indices, not for
                # #      the 'portal' indices, which hold transformed content.

                # # get URL for the OpenSearch server
                # es_url = self.INDICES['indices']['entities']['elasticsearch']['url'].strip('/')

                # # Get the ancestors and descendants of this entity as they exist in Neo4j, and as they
                # # exist in OpenSearch.
                # neo4j_ancestor_ids = self.call_entity_api(entity_id=entity_id
                #                                           , endpoint_base='ancestors'
                #                                           , endpoint_suffix=None
                #                                           , url_property='uuid')
                # neo4j_descendant_ids = self.call_entity_api(entity_id=entity_id
                #                                             , endpoint_base='descendants'
                #                                             , endpoint_suffix=None
                #                                             , url_property='uuid')
                # # If working with a Dataset, it may be copied into ElasticSearch documents for
                # # Collections and Uploads, so identify any of those which must be reindexed.
                # neo4j_collection_ids = []
                # neo4j_upload_ids = []
                # if entity['entity_type'] == 'Dataset':
                #     neo4j_collection_ids = self.call_entity_api(entity_id=entity_id
                #                                                 , endpoint_base='entities'
                #                                                 , endpoint_suffix='collections'
                #                                                 , url_property='uuid')
                #     neo4j_upload_ids = self.call_entity_api(entity_id=entity_id
                #                                             , endpoint_base='entities'
                #                                             , endpoint_suffix='uploads'
                #                                             , url_property='uuid')

                # # Use the index with documents for all entities to determine the relationships of the
                # # current entity as stored in OpenSearch.  Consider it safe to assume documents in other
                # # indices for the same entity have exactly the same relationships unless there was an
                # # indexing problem.
                # #
                # # "Changed relationships" only applies to differences in the ancestors and descendants of
                # # an entity. Uploads and Collections which reference a Dataset entity, for example, do not
                # # indicate a change of relationships which would result in reindexing instead of directly updating.
                # index_with_everything = self.INDICES['indices']['entities']['private']
                # existing_entity_json = self._get_existing_entity_relationships(entity_uuid=entity['uuid']
                #                                                                , es_url=es_url
                #                                                                , es_index=index_with_everything)

                # relationships_changed = self._relationships_changed_since_indexed(neo4j_ancestor_ids=neo4j_ancestor_ids
                #                                                                   , neo4j_descendant_ids=neo4j_descendant_ids
                #                                                                   , existing_oss_doc=existing_entity_json)

                # # Now that it has been determined whether relationships have changed for this entity,
                # # reindex the entity itself first before dealing with other documents for related entities.
                # self._call_indexer(entity=entity
                #                    , delete_existing_doc_first=True)

                # if relationships_changed:
                #     logger.info(f"Related entities for {entity_id} have changed in Neo4j. Reindexing")
                #     # Since the entity is new or the Neo4j relationships with related entities have changed,
                #     # reindex the current entity
                #     self._reindex_related_entities(entity_id=entity_id
                #                                    , entity_type=entity['entity_type']
                #                                    , neo4j_ancestor_ids=neo4j_ancestor_ids
                #                                    , neo4j_descendant_ids=neo4j_descendant_ids
                #                                    , neo4j_collection_ids=neo4j_collection_ids
                #                                    , neo4j_upload_ids=neo4j_upload_ids)
                # else:
                #     logger.info(f"Related entities for {entity_id} are unchanged in Neo4j."
                #                 f" Directly updating index docs of related entities.")
                #     # Since the entity's relationships are identical in Neo4j and OpenSearch, just update
                #     # documents in the entities indices with a copy of the current entity.
                #     for es_index in [   self.INDICES['indices']['entities']['private']
                #                             ,self.INDICES['indices']['entities']['public'] ]:
                #         # Since _directly_modify_related_entities() will only _update documents which already
                #         # exist in an index, no need to test if this entity belongs in the public index.
                #         self._directly_modify_related_entities( es_url=es_url
                #                                                 , es_index=es_index
                #                                                 , entity_id=entity_id
                #                                                 , neo4j_ancestor_ids=neo4j_ancestor_ids
                #                                                 , neo4j_descendant_ids=neo4j_descendant_ids
                #                                                 , neo4j_collection_ids= neo4j_collection_ids
                #                                                 , neo4j_upload_ids=neo4j_upload_ids)

                #     # Until the portal indices support direct updates using correctly transformed documents,
                #     # continue doing a reindex for the updated entity and all the related entities which
                #     # copy data from it for their documents.
                #     previous_revision_ids = []
                #     next_revision_ids = []

                #     # Only Dataset/Publication entities may have previous/next revisions
                #     if entity['entity_type'] in ['Dataset', 'Publication']:
                #         previous_revision_ids = self.call_entity_api(entity_id=entity_id,
                #                                                      endpoint_base='previous_revisions',
                #                                                      endpoint_suffix=None, url_property='uuid')
                #         next_revision_ids = self.call_entity_api(entity_id=entity_id,
                #                                                  endpoint_base='next_revisions',
                #                                                  endpoint_suffix=None, url_property='uuid')

                #     # All unique entity ids in the path excluding the entity itself
                #     target_ids = set(neo4j_ancestor_ids + neo4j_descendant_ids + previous_revision_ids +
                #                      next_revision_ids + neo4j_collection_ids + neo4j_upload_ids)

                #     # Reindex the entity, and all related entities which have details of
                #     # this entity in their document.
                #     self._transform_and_write_entity_to_index_group(entity=entity
                #                                                     , index_group='portal')
                #     with concurrent.futures.ThreadPoolExecutor() as executor:
                #         futures_list = [executor.submit(self._exec_reindex_entity_to_index_group_by_id, related_entity_uuid, 'portal') for related_entity_uuid in target_ids]


                logger.info(f"Finished executing translate() on {entity['entity_type']} of uuid: {entity_id}")
        except Exception:
            msg = "Exceptions during executing translate()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def update(self, entity_id, document, index=None, scope=None):
        if index is not None and index == 'files':
            # The "else clause" is the dominion of the original flavor of OpenSearch indices, for which search-api
            # was created.  This clause is specific to 'files' indices, by virtue of the conditions and the
            # following assumption that dataset_uuid is on the JSON body.
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
            # following assumption that dataset_uuid is on the JSON body.
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

    # This method is only applied to Collection/Donor/Sample/Dataset/File
    # Collection uses entity-api's logic for "visibility" to determine if a Collection is public or nonpublic
    # For File, if the Dataset of the dataset_uuid element has status=='Published', it may go in a public index
    # For Dataset, if status=='Published', it goes into the public index
    # For Donor/Sample, `data`if any dataset down in the tree is 'Published', they should have `data_access_level` as public,
    # then they go into public index
    # Don't confuse with `data_access_level`
    def is_public(self, document):
        is_public = False

        if 'file_uuid' in document:
            # Confirm the Dataset to which the File entity belongs is published
            dataset = self.call_entity_api(document['dataset_uuid'], 'documents')
            return self.is_public(dataset)

        if document['entity_type'] in ['Dataset', 'Publication']:
            # In case 'status' not set
            if 'status' in document:
                if document['status'].lower() == self.DATASET_STATUS_PUBLISHED:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(f"{document['entity_type']} of uuid: {document['uuid']} missing 'status' property, treat as not public, verify and set the status.")
        elif document['entity_type'] in ['Collection', 'Epicollection']:
            # If this Collection meets entity-api's criteria for visibility to the world by
            # returning the value of its schema_constants.py DataVisibilityEnum.PUBLIC,
            # the Collection can be in the public index and retrieved by users who are not logged in.
            entity_visibility = self.call_entity_api(document['uuid'], 'visibility')
            is_public = (entity_visibility == "public")
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
                    theEntity = self.call_entity_api(entity_id, 'documents')
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
            upload = self.call_entity_api(entity_id=entity_id, endpoint_base='documents')

            self._add_datasets_to_entity(   entity=upload
                                            , index_group=self.DEFAULT_INDEX_WITHOUT_PREFIX)
            self._entity_keys_rename(upload)

            # Add additional calculated fields if any applies to Upload
            self.add_calculated_fields(upload)

            self._index_doc_directly_to_es_index(   entity=upload
                                                    , document=json.dumps(upload)
                                                    , es_index=default_private_index
                                                    , delete_existing_doc_first=reindex)

            logger.info(f"Finished executing translate_upload() for {entity_id}")
        except Exception as e:
            logger.error(e)

    def translate_collection(self, entity_id, reindex=False):
        logger.info(f"Start executing translate_collection() for {entity_id}")

        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        try:
            for index_group in self.indices.keys():
                collection = self.get_collection_doc(entity_id=entity_id)

                self._add_datasets_to_entity(   entity=collection
                                                , index_group=index_group)
                self._entity_keys_rename(collection)

                # Add additional calculated fields if any applies to Collection
                self.add_calculated_fields(collection)

                # each index should have a public index
                public_index = self.INDICES['indices'][index_group]['public']
                private_index = self.INDICES['indices'][index_group]['private']

                # If this Collection meets entity-api's criteria for visibility to the world by
                # returning the value of its schema_constants.py DataVisibilityEnum.PUBLIC, put
                # the Collection in the public index.
                # If the index group has a transformer use to retrieve a modified version of
                # the Collection entity to index.
                coll_data = copy.deepcopy(collection)
                if self.TRANSFORMERS.get(index_group):
                    coll_data = self.TRANSFORMERS[index_group].transform(collection, self.transformation_resources)
                if self.is_public(collection):
                    # Remove fields explicitly marked for excluded_properties_from_public_response per entity type in
                    # the provenance_schema.yaml of the entity-api.
                    pub_coll_data = copy.deepcopy(coll_data)
                    if pub_coll_data['entity_type'] in self.public_doc_exclusion_dict:
                        self._remove_field_from_dict(a_dict=pub_coll_data
                                                     , obj_to_remove=self.public_doc_exclusion_dict[pub_coll_data['entity_type']])
                    self._index_doc_directly_to_es_index(entity=pub_coll_data
                                                         , document=json.dumps(pub_coll_data)
                                                         , es_index=public_index
                                                         , delete_existing_doc_first=reindex)
                self._index_doc_directly_to_es_index(entity=coll_data
                                                     , document=json.dumps(coll_data)
                                                     , es_index=private_index
                                                     , delete_existing_doc_first=reindex)

            logger.info(f"Finished executing translate_collection() for {entity_id}")
        except requests.exceptions.RequestException as e:
            logger.exception(e)
            # Log the error and will need fix later and reindex, rather than sys.exit()
            logger.error(f"translate_collection() failed to get collection of uuid: {entity_id} via entity-api")
        except Exception as e:
            logger.error(e)

    def translate_donor_tree(self, entity_id):
        try:
            logger.info(f"Start executing translate_donor_tree() for donor of uuid: {entity_id}")

            descendant_uuids = self.call_entity_api(entity_id=entity_id
                                                    , endpoint_base='descendants'
                                                    , endpoint_suffix=None
                                                    , url_property='uuid')

            # Index the donor entity itself
            donor = self.call_entity_api(entity_id, 'documents')
            self._call_indexer(entity=donor)

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

        entity_dict = self.call_entity_api(uuid, 'documents')
        self._call_indexer(entity=entity_dict)

        logger.info(f"Finished executing index_entity() on uuid: {uuid}")

    # Used by individual PUT /reindex/<id> call
    def reindex_entity(self, uuid):
        logger.info(f"Start executing reindex_entity() on uuid: {uuid}")

        entity_dict = self.call_entity_api(uuid, 'documents')
        self._call_indexer(entity=entity_dict, delete_existing_doc_first=True)

        logger.info(f"Finished executing reindex_entity() on uuid: {uuid}")

    def load_public_doc_exclusion_dict(self, entity_api_prov_schema_raw_url):
        # Keep a semi-immutable dictionary of fields to exclude from public indices, using the
        # same information entity-api uses for excluding fields for public entities.
        response = requests.get(    url=entity_api_prov_schema_raw_url
                                    , verify=False)
        if response.status_code == 200:
            yaml_contents = response.text
            try:
                provenance_schema_dict = MappingProxyType(safe_load(yaml_contents))
            except YAMLError as ye:
                raise YAMLError(ye)
        else:
            msg = f"Unable to retrieve public index field exclusion information"
            self.logger.error(  f"{msg}."
                                f" Got an HTTP {response.status_code}"
                                f" retrieving {self.indices['entity_api_prov_schema_raw_url']}")
            raise HTTPException(f"{msg}. See logs.")
        if not provenance_schema_dict or 'ENTITIES' not in provenance_schema_dict:
            msg = f"Unable retrieve Entity API's provenance_schema.yaml information"
            self.logger.error(  f"{msg}."
                                f" Not expected content using the translator's"
                                f" self.indices['entity_api_prov_schema_raw_url']={self.indices['entity_api_prov_schema_raw_url']}.")
            raise Exception(f"{msg}. See logs.")
        self.public_doc_exclusion_dict={}
        for k,v in provenance_schema_dict['ENTITIES'].items():
            if 'excluded_properties_from_public_response' in v:
                self.public_doc_exclusion_dict[k]=v['excluded_properties_from_public_response']
            else:
                # ENTITIES entries which do not have an excluded_properties_from_public_response field may
                # still be supplemented by the supplement_public_doc_exclusion_dict() method, so need an
                # empty list that can be appended to.
                self.public_doc_exclusion_dict[k]=[]

    def supplement_public_doc_exclusion_dict(self):
        # Get a snapshot of the exclusions loaded from Entity API YAML for each
        # entity type, prior to supplementing.
        base_entity_exclusions={}
        for entity_type, entity_exclusions in self.public_doc_exclusion_dict.items():
            base_entity_exclusions[entity_type] = copy.deepcopy(entity_exclusions)

        # For Samples, exclude the same fields under "origin_samples" which are
        # excluded for the Sample entity's base fields
        self.public_doc_exclusion_dict['Sample'].append({'origin_samples': base_entity_exclusions['Sample']})

        # For Samples, exclude the same fields under "donor" which are
        # excluded for Donor entities.
        self.public_doc_exclusion_dict['Sample'].append({'donor': base_entity_exclusions['Donor']})

        # For Datasets, exclude the same fields under "donor" which are
        # excluded for Donor entities.
        self.public_doc_exclusion_dict['Dataset'].append({'donor': base_entity_exclusions['Donor']})

        # For Datasets, exclude the same fields under "origin_samples" which are
        # excluded for the Sample entity's base fields
        self.public_doc_exclusion_dict['Dataset'].append({'origin_samples': base_entity_exclusions['Sample']})

        # For Datasets, exclude the same fields under "source_samples" which are
        # excluded for the Sample entity's base fields
        self.public_doc_exclusion_dict['Dataset'].append({'source_samples': base_entity_exclusions['Sample']})

        # For EPICollection, Collection, and Upload entities, exclude the same fields under "datasets" which are
        # excluded for Dataset entities
        self.public_doc_exclusion_dict['Epicollection'].append({'datasets': base_entity_exclusions['Dataset']})
        self.public_doc_exclusion_dict['Collection'].append({'datasets': base_entity_exclusions['Dataset']})


        # self.public_doc_exclusion_dict={}
        # for k,v in provenance_schema_dict['ENTITIES'].items():
        #     if 'excluded_properties_from_public_response' in v:
        #         self.public_doc_exclusion_dict[k]=v['excluded_properties_from_public_response']

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


   
    # # Commented out by Zhou to avoid 409 conflicts - 7/20/2024
    # def _reindex_related_entities(  self, entity_id:str, entity_type:str, neo4j_ancestor_ids:list[str]
    #                                 , neo4j_descendant_ids:list[str], neo4j_collection_ids:list[str]
    #                                 , neo4j_upload_ids:list[str]):
    #     # If entity is new or Neo4j relationships for entity have changed, do a reindex with each ID
    #     # which has entity as an ancestor or descendant.  This is a costlier operation than
    #     # directly updating documents for related entities.
    #     previous_revision_ids = []
    #     next_revision_ids = []

    #     # Only Dataset/Publication entities may have previous/next revisions
    #     if entity_type in ['Dataset', 'Publication']:
    #         previous_revision_ids = self.call_entity_api(entity_id=entity_id, endpoint_base='previous_revisions', endpoint_suffix=None, url_property='uuid')
    #         next_revision_ids = self.call_entity_api(entity_id=entity_id, endpoint_base='next_revisions', endpoint_suffix=None, url_property='uuid')

    #     # All unique entity ids which might refer to the entity of entity_id
    #     target_ids = set(neo4j_ancestor_ids + neo4j_descendant_ids + previous_revision_ids + next_revision_ids +
    #                      neo4j_collection_ids + neo4j_upload_ids)

    #     # Reindex the rest of the entities in the list
    #     with concurrent.futures.ThreadPoolExecutor() as executor:
    #         futures_list = [executor.submit(self.reindex_entity, uuid) for uuid in target_ids]
    #         for f in concurrent.futures.as_completed(futures_list):
    #             result = f.result()



    # Note: this entity dict input (if Dataset) has already removed ingest_metadata.files and
    # ingest_metadata.metadata sub fields with empty string values from previous call
    def _index_doc_directly_to_es_index(self, entity:dict, document:json, es_index:str, delete_existing_doc_first:bool=False):
        logger.info(f"Start executing _index_doc_directly_to_es_index() on uuid: {entity['uuid']}, entity_type: {entity['entity_type']}")

        try:
            self.indexer.index(entity['uuid'], document, es_index, reindex=delete_existing_doc_first)
            logger.info(f"Finished executing _index_doc_directly_to_es_index() on uuid: {entity['uuid']}, entity_type: {entity['entity_type']}")
        except Exception as e:
            msg =   f"Encountered exception e={str(e)}" \
                    f" executing _index_doc_directly_to_es_index() with" \
                    f" uuid: {entity['uuid']}, entity_type: {entity['entity_type']}" \
                    f" es_index={es_index}"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # Note: this entity dict input (if Dataset) has already removed ingest_metadata.files and
    # ingest_metadata.metadata sub fields with empty string values from previous call
    def _call_indexer(self, entity, delete_existing_doc_first=False):
        logger.info(f"Start executing _call_indexer() on uuid: {entity['uuid']}, entity_type: {entity['entity_type']}")

        try:
            # Generate and write a document for the entity to each index group loaded from the configuration file.
            for index_group in self.indices.keys():
                self._transform_and_write_entity_to_index_group(entity=entity
                                                                , index_group=index_group)
            logger.info(f"Finished executing _call_indexer() on uuid: {entity['uuid']}, entity_type: {entity['entity_type']}")
        except Exception as e:
            msg = f"Encountered exception e={str(e)}" \
                  f" executing _call_indexer() with" \
                  f" uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

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

    # The calculated fields added to an entity by add_calculated_fields() should not be added
    # to themselves as subfields. Remove them, similarly to exclude_added_top_level_properties()
    def exclude_added_calculated_fields(self, entity_data, except_properties_list = []):
        logger.info("Start executing exclude_added_calculated_fields()")

        if 'index_version' in entity_data:
            entity_data.pop('index_version')
        if 'display_subtype' in entity_data:
            entity_data.pop('display_subtype')

        logger.info("Finished executing exclude_added_calculated_fields()")

    # Used for Upload and Collection index
    def _add_datasets_to_entity(self, entity:dict, index_group:str):
        logger.info("Start executing _add_datasets_to_entity()")

        datasets = []
        if 'datasets' in entity:
            for dataset in entity['datasets']:
                # Retrieve the entity details
                try:
                    dataset = self.call_entity_api(dataset['uuid'], 'documents')
                    # Remove large fields that cause poor performance and are not used, both for current
                    # implementation and ingest_metadata reorganization is coordinated for Production release,
                    # will also remove 'files' here, and delete the call to exclude_added_top_level_properties() below.
                    for large_field_name in NESTED_EXCLUDED_ES_FIELDS_FOR_COLLECTIONS_AND_UPLOADS:
                        if large_field_name in dataset:
                            dataset.pop(large_field_name)
                except Exception as e:
                    logger.exception(e)
                    logger.error(   f"Failed to retrieve dataset {dataset['uuid']}"
                                    f" via entity-api while executing"
                                    f" _add_datasets_to_entity(). Skip and continue to next one")
                    
                    # This can happen when the dataset is in neo4j but the actual uuid is not found in MySQL
                    # or something else is wrong with entity-api and it can't return the dataset info
                    # In this case, we'll skip over the current iteration, and continue with the next one
                    # Otherwise, null will be added to the resulting datasets list and break portal-ui rendering - 5/3/2023 Zhou
                    continue
                
                try:
                    dataset_doc = self._generate_doc(   entity=dataset
                                                        , return_type='dict'
                                                        , index_group=index_group)
                except Exception as e:
                    logger.exception(e)
                    logger.error(   f"Failed to execute _generate_doc() on dataset {dataset['uuid']}"
                                    f" while executing _add_datasets_to_entity()."
                                    f" Skip and continue to next one")

                    # This can happen when the dataset itself is good but something failed to generate the doc
                    # E.g., one of the descendants of this dataset exists in neo4j but no record in uuid MySQL
                    # In this case, we'll skip over the current iteration, and continue with the next one
                    # Otherwise, no document is generated, null will be added to the resuting datasets list and break portal-ui rendering - 5/3/2023 Zhou
                    continue
                self.exclude_added_top_level_properties(dataset_doc)
                datasets.append(dataset_doc)

        entity['datasets'] = datasets

        logger.info("Finished executing _add_datasets_to_entity()")

    # Modify any key names specified to change on the entity
    def _entity_keys_rename(self, entity):
        logger.info("Start executing _entity_keys_rename()")

        # Special case of Sample.rui_location
        # To be backward compatible for API clients relying on the old version
        # Also gives the ES consumer flexibility to change the inner structure
        # Note: when `rui_location` is stored as json object (Python dict) in ES
        # with the default dynamic mapping, it can cause errors due to
        # the changing data types of some internal fields
        # isinstance() check is to avoid json.dumps() on json string again
        if 'rui_location' in entity and isinstance(entity['rui_location'], dict):
            entity['rui_location'] = json.dumps(entity['rui_location'])

        logger.info("Finished executing _entity_keys_rename()")

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
    # otherwise sample_category code as the display name for Block, Section, or Suspension.
    # Dataset: the display names linked to the values in dataset_type as a comma separated list
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
                        known_organ_types = self.transformation_resources['organ_map']
                        if entity['organ'] in known_organ_types.keys():
                            display_subtype = known_organ_types[entity['organ']].get('term')
                        else:
                            raise Exception(
                                f"Unable retrieve organ type ontology information for organ_type_code={entity['organ']}.")
                    else:
                        logger.error(
                            f"Missing missing organ when sample_category is set of Sample with uuid: {entity['uuid']}")
                else:
                    display_subtype = str.capitalize(entity['sample_category'])
            else:
                logger.error(f"Missing sample_category of Sample with uuid: {entity['uuid']}")
        elif entity_type in ['Dataset', 'Publication']:
            if 'dataset_type' in entity:
                display_subtype = entity['dataset_type']
            else:
                logger.error(f"Missing dataset_type of Dataset with uuid: {entity['uuid']}")
        else:
            # Do nothing
            logger.error(
                f"Invalid entity_type: {entity_type}. Only generate display_subtype for Upload/Donor/Sample/Dataset")

        logger.info("Finished executing generate_display_subtype()")

        return display_subtype


    # Make a descendant or ancestor list specific to the needs of an index groups documents
    def _relatives_for_index_group(self, relative_ids:list, index_group:str):
        relatives_for_index_group = []
        for relative_uuid in relative_ids:
            relative_dict = self.call_entity_api(relative_uuid, 'documents')
            # Only retain the elements of each relative needed for the index group
            entity_relative_dict = {}
            ig_doc_fields = INDEX_GROUP_PORTAL_DOC_FIELDS if index_group == 'portal' else INDEX_GROUP_ENTITIES_DOC_FIELDS
            for desc_key in ig_doc_fields.keys():
                if desc_key in relative_dict.keys():
                    entity_relative_dict[desc_key] = relative_dict[desc_key]
            relatives_for_index_group.append(entity_relative_dict)
        return relatives_for_index_group

    # Note: this entity dict input (if Dataset) has already handled ingest_metadata.files (with empty string or missing)
    # and ingest_metadata.metadata sub fields with empty string values from previous call
    def _generate_doc(self, entity, return_type, index_group:str):
        try:
            logger.info(f"Start executing _generate_doc() for {entity['entity_type']}"
                        f" of uuid: {entity['uuid']}"
                        f" for the {index_group} index group.")

            entity_id = entity['uuid']

            # Full ancestors may not be needed to populate a field in an ES index of
            # an index group, but fill for now to calculate other fields e.g. origin_samples
            ancestors = []
            # The ES document top-level "donor" field will be the first ancestor of
            # this entity with an entity_type of 'Donor'.
            donor = None
            if entity['entity_type'] != 'Upload':
                # Do not call /ancestors/<id> directly to avoid performance/timeout issue
                # Get back a list of ancestor uuids first
                ancestor_ids = self.call_entity_api(entity_id=entity_id
                                                    , endpoint_base='ancestors'
                                                    , endpoint_suffix=None
                                                    , url_property='uuid')
                # Fill ancestors with "full" entities, both for use in calculating 'origin_samples' below and
                # to determine which ancestor populates 'donor'.  But after all calculations, cut 'ancestors'
                # back to the specific needs for documents in the index group
                for ancestor_uuid in ancestor_ids:
                    ancestor_dict = self.call_entity_api(entity_id=ancestor_uuid
                                                         , endpoint_base='documents')
                    ancestors.append(ancestor_dict)

                    # If the current ancestor is the first Donor encountered, save it to
                    # populate the ES document "donor" field for this entity.
                    if ancestor_dict['entity_type'] == 'Donor' and not donor:
                        donor = copy.deepcopy(ancestor_dict)

                descendant_ids = self.call_entity_api(entity_id=entity_id
                                                    , endpoint_base='descendants'
                                                    , endpoint_suffix=None
                                                    , url_property='uuid')
                descendants = self._relatives_for_index_group(  relative_ids=descendant_ids
                                                                , index_group=index_group)

                immediate_ancestor_ids = self.call_entity_api(entity_id=entity_id
                                                            , endpoint_base='parents'
                                                            , endpoint_suffix=None
                                                            , url_property='uuid')

                immediate_descendant_ids = self.call_entity_api(entity_id=entity_id
                                                                , endpoint_base='children'
                                                                , endpoint_suffix=None
                                                                , url_property='uuid')

                # Add new properties to entity for documents in all indices
                entity['descendants'] = descendants
                entity['ancestor_ids'] = ancestor_ids
                entity['descendant_ids'] = descendant_ids
                entity['immediate_ancestor_ids'] = immediate_ancestor_ids
                entity['immediate_descendant_ids'] = immediate_descendant_ids

                # Add new properties to entity only needed for documents in the 'portal' index group
                if index_group == 'portal':
                    immediate_ancestors = []
                    immediate_descendants = []

                    for immediate_ancestor_uuid in immediate_ancestor_ids:
                        immediate_ancestor_dict = self.call_entity_api(immediate_ancestor_uuid, 'documents')
                        immediate_ancestors.append(immediate_ancestor_dict)
                    index_group_immediate_ancestors = self._relatives_for_index_group(  relative_ids=immediate_ancestor_ids
                                                                                        , index_group=index_group)
                    entity['immediate_ancestors'] = index_group_immediate_ancestors

                    for immediate_descendant_uuid in immediate_descendant_ids:
                        immediate_descendant_dict = self.call_entity_api(immediate_descendant_uuid, 'documents')
                        immediate_descendants.append(immediate_descendant_dict)
                    index_group_immediate_descendants = self._relatives_for_index_group(relative_ids=immediate_descendant_ids
                                                                                        , index_group=index_group)
                    entity['immediate_descendants'] = index_group_immediate_descendants

            # The `sample_category` is "organ" and the `organ` code is set at the same time
            if entity['entity_type'] in ['Sample', 'Dataset', 'Publication']:
                # Add new properties
                if donor:
                    entity['donor'] = donor

                # entity['origin_samples'] is a list
                entity['origin_samples'] = []
                if ('sample_category' in entity) and (entity['sample_category'].lower() == 'organ') and ('organ' in entity) and (entity['organ'].strip() != ''):
                    entity['origin_samples'].append(copy.deepcopy(entity))
                else:
                    for ancestor in ancestors:
                        if ('sample_category' in ancestor) and (ancestor['sample_category'].lower() == 'organ') and ('organ' in ancestor) and (ancestor['organ'].strip() != ''):
                            entity['origin_samples'].append(ancestor)

                # Remove those added fields specified in `entity_properties_list` from origin_samples
                self.exclude_added_top_level_properties(entity['origin_samples'])
                # Remove calculated fields added to a Sample from 'origin_samples'
                for origin_sample in entity['origin_samples']:
                    self.exclude_added_calculated_fields(origin_sample)

                # `source_samples` field is only available to Dataset
                if entity['entity_type'] in ['Dataset', 'Publication']:
                    entity['source_samples'] = None
                    e = entity

                    while entity['source_samples'] is None:
                        parent_uuids = self.call_entity_api(entity_id=e['uuid']
                                                            , endpoint_base='parents'
                                                            , endpoint_suffix=None
                                                            , url_property='uuid')
                        parents = []
                        for parent_uuid in parent_uuids:
                            parent_entity_doc = self.call_entity_api(entity_id=parent_uuid
                                                                    , endpoint_base='documents')
                            parents.append(parent_entity_doc)

                        try:
                            if parents[0]['entity_type'] == 'Sample':
                                # If one parent entity of this Dataset is a Sample, then all parent entities
                                # of this Dataset must be Samples.
                                entity['source_samples'] = parents
                            e = parents[0]
                        except IndexError:
                            entity['source_samples'] = []

            # Now that calculations use 'ancestors' with fully populated entities are
            # complete, set entity['ancestors'] instead to a value appropriate for
            # the index group, prior to any renaming or removal operations.
            entity['ancestors'] = self._relatives_for_index_group(  relative_ids=ancestor_ids
                                                                    , index_group=index_group)

            self._entity_keys_rename(entity)

            # Rename for properties that are objects
            if entity.get('donor', None):
                self._entity_keys_rename(entity['donor'])

            if entity.get('origin_samples', None):
                for o in entity.get('origin_samples', None):
                    self._entity_keys_rename(o)
            if entity.get('source_samples', None):
                for s in entity.get('source_samples', None):
                    self._entity_keys_rename(s)
            if entity.get('ancestors', None):
                for a in entity.get('ancestors', None):
                    self._entity_keys_rename(a)
            if entity.get('descendants', None):
                for d in entity.get('descendants', None):
                    self._entity_keys_rename(d)
            if entity.get('immediate_descendants', None):
                for parent in entity.get('immediate_descendants', None):
                    self._entity_keys_rename(parent)
            if entity.get('immediate_ancestors', None):
                for child in entity.get('immediate_ancestors', None):
                    self._entity_keys_rename(child)

            remove_specific_key_entry(entity, "other_metadata")

            # Add additional calculated fields
            self.add_calculated_fields(entity)

            # Establish the list of fields which should be removed from top-level fields prior to
            # writing the entity as an ElasticSearch document.
            ig_doc_fields=INDEX_GROUP_PORTAL_DOC_FIELDS if index_group=='portal' else INDEX_GROUP_ENTITIES_DOC_FIELDS
            unretained_key_list = [k for k, v in ig_doc_fields.items() if v != PropertyRetentionEnum.ES_DOC]

            # We need to leave fields in unretained_key_list on the instance of
            # entity we are modifying for use by _generate_public_doc(), but we also
            # do not want these fields in the ElasticSearch document.  So make a
            # deepcopy of entity, remove fields from it, and use it to return a value
            doc_entity = copy.deepcopy(entity)
            for top_level_field in {'ancestors', 'immediate_ancestors', 'descendants', 'immediate_descendants'}:
                if top_level_field in doc_entity:
                    for field_of_top_level_field in unretained_key_list:
                        remove_specific_key_entry(  obj=doc_entity[top_level_field]
                                                    , key_to_remove=field_of_top_level_field)
                    # After removing unneeded entries within members of the top_level_field, remove
                    # the member itself if it is empty. However, keep the top_level_field in
                    # the entity even if it is empty.
                    for index, value in enumerate(doc_entity[top_level_field]):
                        if not value:
                           doc_entity[top_level_field].pop(index)

            logger.info(f"Finished executing _generate_doc() for {doc_entity['entity_type']}"
                        f" of uuid: {doc_entity['uuid']}"
                        f" for the {index_group} index group.")

            return json.dumps(doc_entity) if return_type == 'json' else doc_entity
        except Exception as e:
            msg = "Exceptions during executing hubmap_translator._generate_doc()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            # Raise the exception so the caller can handle it properly
            raise Exception(e)

    def _generate_public_doc(self, entity, index_group:str):
        # N.B. This method assumes the state of the 'entity' argument has been processed by the
        #      _generate_doc() function, so this method should always be called after that one.
        logger.info(f"Start executing _generate_public_doc() for {entity['entity_type']}"
                    f" of uuid: {entity['uuid']}"
                    f" for the {index_group} index group.")

        # Only Dataset has this 'next_revision_uuid' property
        property_key = 'next_revision_uuid'
        if (entity['entity_type'] in ['Dataset', 'Publication']) and (property_key in entity):
            next_revision_uuid = entity[property_key]
            
            # Can't reuse call_entity_api() here due to the response data type
            # Making a call against entity-api/entities/<next_revision_uuid>?property=status
            url = self.entity_api_url + "/entities/" + next_revision_uuid + "?property=status"
            response = requests.get(url, headers=self.request_headers, verify=False)

            if response.status_code != 200:
                logger.error(f"_generate_public_doc() failed to get Dataset/Publication status of next_revision_uuid via entity-api for uuid: {next_revision_uuid}")
                
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

        descendants = self._relatives_for_index_group(  relative_ids=[e['uuid'] for e in entity['descendants']]
                                                        , index_group=index_group)
        entity['descendants'] = list(filter(self.is_public, descendants))

        # Add new properties to entity only needed for documents in the 'portal' index group
        if index_group == 'portal':
            entity['immediate_descendants'] = list(filter(self.is_public, entity['immediate_descendants']))

        # Establish the list of fields which should be removed from top-level fields prior to
        # writing the entity as an ElasticSearch document.
        ig_doc_fields = INDEX_GROUP_PORTAL_DOC_FIELDS if index_group == 'portal' else INDEX_GROUP_ENTITIES_DOC_FIELDS
        unretained_key_list = [k for k, v in ig_doc_fields.items() if v != PropertyRetentionEnum.ES_DOC]

        # Remove fields explicitly marked for excluded_properties_from_public_response per entity type in
        # the provenance_schema.yaml of the entity-api.
        if entity['entity_type'] in self.public_doc_exclusion_dict:
            self._remove_field_from_dict(   a_dict=entity
                                            , obj_to_remove=self.public_doc_exclusion_dict[entity['entity_type']])

        # Because _generate_doc() left some fields on the entity which should not be a part of the
        # ElasticSearch document, but which were needed for calculations prior to now, remove them before
        # returning the public document contents.
        for top_level_field in {'ancestors', 'immediate_ancestors', 'descendants', 'immediate_descendants'}:
            if top_level_field in entity:
                for field_of_top_level_field in unretained_key_list:
                    remove_specific_key_entry(obj=entity[top_level_field]
                                              , key_to_remove=field_of_top_level_field)

        logger.info(f"Finished executing _generate_public_doc() for {entity['entity_type']} of uuid: {entity['uuid']}")

        return json.dumps(entity)

    """
    Retrieves fields designated in the provenance schema yaml under 
    excluded_properties_from_public_response and returns the fields in a list

    Parameters
    ----------
    normalized_class : str
        the normalized entity type of the entity who's fields are to be removed

    Returns
    -------
    excluded_fields
        A list of strings where each entry is a field to be excluded
    """

    def get_fields_to_exclude(self, normalized_class=None):
        # Determine the schema section based on class
        excluded_fields = []
        schema_section = self.INDICES['indices'] # _schema['ENTITIES']
        exclude_list = schema_section[normalized_class].get('excluded_properties_from_public_response')
        if exclude_list:
            excluded_fields.extend(exclude_list)
        return excluded_fields

    # Remove a string field from a dictionary, or call recursively if
    # the object passed in is a dict or list rather than a str.
    def _remove_field_from_dict(self, a_dict:dict, obj_to_remove):
        # Most of the code of this method is designed to work with a dict, so if
        # given a list instead, apply obj_to_remove to each element of the list
        if isinstance(a_dict, list):
            # a_dict will shift during recursion, which allows assignment. But preserve
            # pointing at the list passed in, so it can be restored after processing each
            # element of the list
            original_a_dict = a_dict
            for list_entry in a_dict:
                list_entry = self._remove_field_from_dict(  a_dict=list_entry
                                                            , obj_to_remove=obj_to_remove)
            a_dict = original_a_dict
        elif isinstance(obj_to_remove, str):
            if obj_to_remove in a_dict:
                a_dict.pop(obj_to_remove)
        elif isinstance(obj_to_remove, list):
            for obj in obj_to_remove:
                # Recursively remove each element in the list
                a_dict = self._remove_field_from_dict(  a_dict=a_dict
                                                        , obj_to_remove=obj)
        elif isinstance(obj_to_remove, dict):
            for k, v in obj_to_remove.items():
                # Recursively process the value for each dictionary key
                if k in a_dict:
                    a_dict[k] = self._remove_field_from_dict(  a_dict=a_dict[k]
                                                            , obj_to_remove=v)
        else:
            logger.error(f"Unable to process obj_to_remove.type()={obj_to_remove.type()}")
            raise Exception('Error generating public document. See logs.')
        return a_dict

    # This method is supposed to only retrieve Dataset|Donor|Sample
    # The Collection and Upload are handled by separate calls
    # The returned data can either be an entity dict or a list of uuids (when `url_property` parameter is specified)
    def call_entity_api(self, entity_id, endpoint_base, endpoint_suffix=None, url_property=None):
        logger.info(f"Start executing call_entity_api() on uuid: {entity_id}")

        url = f"{self.entity_api_url}/{endpoint_base}/{entity_id}"
        if endpoint_suffix:
            url = f"{url}/{endpoint_suffix}"
        if url_property:
            url = f"{url}?property={url_property}"

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
        return response.json()

    def get_collection_doc(self, entity_id):
        logger.info(f"Start executing get_collection_doc() on uuid: {entity_id}")

        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        url = self.entity_api_url + "/documents/" + entity_id
        response = requests.get(url, headers=self.request_headers, verify=False)

        if response.status_code != 200:
            msg = f"get_collection_doc() failed to get entity of uuid {entity_id} via entity-api"

            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            logger.debug("======get_collection_doc() status code from entity-api======")
            logger.debug(response.status_code)

            logger.debug("======get_collection_doc() response text from entity-api======")
            logger.debug(response.text)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        collection_dict = response.json()

        logger.info(f"Finished executing get_collection_doc() on uuid: {entity_id}")

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

    """
    Retrieve the organ types from ontology-api.  Typically only used to
    initialize self.transformation_resources['organ_map']
    
    Returns
    -------
    dict
        The available organ types in the following format:
    
        {
            "AO": {
                "code": "C030042",
                "organ_cui": "C0003483",
                "organ_uberon": "UBERON:0000947",
                "rui_code": "AO",
                "sab": "HUBMAP",
                "term": "Aorta"
            },
            "BL": {
                "code": "C030043",
                "organ_cui": "C0005682",
                "organ_uberon": "UBERON:0001255",
                "rui_code": "BL",
                "sab": "HUBMAP",
                "term": "Bladder"
            },
            ...
        }
    """
    def get_organ_types(self):
        target_url = f"{self._ontology_api_base_url}{self.ONTOLOGY_API_ORGAN_TYPES_ENDPOINT}"

        # Disable ssl certificate verification, and use the read-only ontology-api without authentication.
        response = requests.get(url=target_url, verify=False)

        # Invoke .raise_for_status(), an HTTPError will be raised with certain status codes
        response.raise_for_status()

        if response.status_code == 200:
            organ_json = response.json()
            return {o['rui_code']: o for o in organ_json}
        else:
            # Log the full stack trace, prepend a line with our message
            logger.exception("Unable to make a request to query the organ types via ontology-api")

            logger.debug("======get_organ_types() status code from ontology-api======")
            logger.debug(response.status_code)

            logger.debug("======get_organ_types() response text from ontology-api======")
            logger.debug(response.text)

            # Also bubble up the error message from ontology-api
            raise requests.exceptions.RequestException(response.text)

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
    translator = Translator(INDICES, app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], token, app.config['ONTOLOGY_API_BASE_URL'])
    
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

    if (len(sys.argv) == 3) and (sys.argv[2] == 'collections'):
        logger.info("############# Collections reindex via script started #############")

        # Do NOT erase any indices, just reindex all collections
        translator.translate_all_collections()

        # Show the failed entity-api calls and the uuids
        if translator.failed_entity_api_calls:
            logger.info(f"{len(translator.failed_entity_api_calls)} entity-api calls failed")
            print(*translator.failed_entity_api_calls, sep = "\n")
     
        if translator.failed_entity_ids:
            logger.info(f"{len(translator.failed_entity_ids)} entity ids failed")
            print(*translator.failed_entity_ids, sep = "\n")

        end = time.time()
        logger.info(f"############# ollections reindex via script completed. Total time used: {end - start} seconds. #############")
    else:
        logger.info("############# Full index via script started #############")

        # Erase all the indices first then index all
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
