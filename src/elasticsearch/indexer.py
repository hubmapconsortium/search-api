import sys
import json
import yaml
import time
import concurrent.futures
import copy
import collections
import requests
import ast
import os
import logging
from datetime import datetime
from pathlib import Path
from yaml import safe_load
from urllib3.exceptions import InsecureRequestWarning
from globus_sdk import AccessTokenAuthorizer, AuthClient
import importlib

# For reusing the app.cfg configuration when running indexer.py as script
from flask import Flask, Response

# Local modules
from libs.es_writer import ESWriter
#from elasticsearch.addl_index_transformations.portal import transform

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons import globus_groups

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

# Set logging fromat and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class Indexer:
    # Class variables/constants
    # All lowercase for easy comparision
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    DATASET_STATUS_PUBLISHED = 'published'
    DEFAULT_INDEX_WITHOUT_PREFIX = ''
    INDICES = {}
    TRANSFORMERS = {}

    # Constructor method with instance variables to be passed in
    def __init__(self, indices, app_client_id, app_client_secret, token):
        try:
            self.indices: dict = {}
            # Do not include the indexes that are self managed...
            for key, value in indices['indices'].items():
                if 'reindex_enabled' in value and value['reindex_enabled'] == 'true':
                    self.indices[key] = value
            self.DEFAULT_INDEX_WITHOUT_PREFIX: str = indices['default_index']
            self.INDICES: dict = {'default_index': self.DEFAULT_INDEX_WITHOUT_PREFIX, indices: self.indices}
            logger.debug("@@@@@@@@@@@@@@@@@@@@ INDICES")
            logger.debug(self.INDICES)
        except Exception:
            raise ValueError("Invalid indices config")

        self.elasticsearch_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['elasticsearch']['url'].strip('/')

        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token

        auth_helper = self.init_auth_helper()
        self.request_headers = self.create_request_headers_for_auth(token)

        self.eswriter = ESWriter(self.elasticsearch_url)
        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')

        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent.parent / 'VERSION').read_text()).strip()

        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'neo4j-to-es-attributes.json'), 'r') as json_file:
            self.attr_map = json.load(json_file)

        # Preload all the transformers
        self.init_transformers()


    # Preload all the transformers if the index has one
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


    def main(self):
        try:
            # load the index configurations and set the default
            self.DEFAULT_INDEX_WITHOUT_PREFIX = self.INDICES['default_index']

            # Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
            DEFAULT_ELASTICSEARCH_URL = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['elasticsearch']['url'].strip('/')
            DEFAULT_ENTITY_API_URL = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')

            # Delete and recreate target indices
            #for index, configs in self.indices['indices'].items():
            for index in self.indices.keys():
                # each index should have a public/private index
                public_index = self.INDICES['indices'][index]['public']
                private_index = self.INDICES['indices'][index]['private']

                try:
                    self.eswriter.delete_index(public_index)
                except Exception as e:
                    pass
                
                try:
                    self.eswriter.delete_index(private_index)
                except Exception as e:
                    pass
                print('*********************************************')                

                # get the specific mapping file for the designated index
                index_mapping_file = self.INDICES['indices'][index]['elasticsearch']['mappings']

                # read the elasticserach specific mappings 
                index_mapping_settings = safe_load((Path(__file__).absolute().parent / index_mapping_file).read_text())

                print(index_mapping_settings)

                print('*********************************************')

                self.eswriter.create_index(public_index, index_mapping_settings)

                print('*********************************************')
                self.eswriter.create_index(private_index, index_mapping_settings)

            # Get a list of public Collection uuids
            url = self.entity_api_url + "/collections?property=uuid"
            response = requests.get(url, headers = self.request_headers, verify = False)
            
            if response.status_code != 200:
                msg = "indexer.main() failed to get all the public Collection uuids via entity-api"
                logger.error(msg)
                sys.exit(msg)

            public_collection_uuids = response.json()

            logger.info(f"Public Collection TOTAL: {len(public_collection_uuids)}")

            # Get a list of Upload uuids
            url = self.entity_api_url + "/upload/entities?property=uuid"
            response = requests.get(url, headers = self.request_headers, verify = False)
            
            if response.status_code != 200:
                msg = "indexer.main() failed to get all the Upload uuids via entity-api"
                logger.error(msg)
                sys.exit(msg)

            upload_uuids = response.json()

            logger.info(f"Upload TOTAL: {len(upload_uuids)}")

            # Get a list of Donor uuids
            url = self.entity_api_url + "/donor/entities?property=uuid"
            response = requests.get(url, headers = self.request_headers, verify = False)
            
            if response.status_code != 200:
                msg = "indexer.main() failed to get all the Donor uuids via entity-api"
                logger.error(msg)
                sys.exit(msg)

            donor_uuids = response.json()

            logger.info(f"Donor TOTAL: {len(donor_uuids)}")

            logger.debug("Starting multi-thread reindexing ...")

            # Iintial index in multi-treading mode for:
            # - each public collection
            # - each upload, only add to the hm_consortium_entities index (private index of the default)
            # - each donor and its descendants in the tree
            futures_list = []
            results = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                public_collection_futures_list = [executor.submit(indexer.index_public_collection, uuid) for uuid in public_collection_uuids]
                upload_futures_list = [executor.submit(indexer.index_upload, uuid) for uuid in upload_uuids]
                donor_futures_list = [executor.submit(indexer.index_tree, uuid) for uuid in donor_uuids]

                # Append the above three lists into one
                futures_list = public_collection_futures_list + upload_futures_list + donor_futures_list
                
                for f in concurrent.futures.as_completed(futures_list):
                    logger.debug(f.result())
        except Exception:
            msg = "Exception encountered during executing indexer.main()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def index_tree(self, donor_uuid):
        # logger.info(f"Total threads count: {threading.active_count()}")

        logger.info(f"Executing index_tree() for donor of uuid: {donor_uuid}")
  
        url = self.entity_api_url + "/descendants/" + donor_uuid + '?property=uuid'
        response = requests.get(url, headers = self.request_headers, verify = False)

        if response.status_code != 200:
            msg = f"indexer.index_tree() failed to get descendant uuids via entity-api for donor of uuid: {donor_uuid}"
            logger.error(msg)
            sys.exit(msg)
        
        descendant_uuids = response.json()

        # Index the donor entity itself separately
        donor = self.get_entity(donor_uuid)

        logger.info(f"indexer.index_tree() for uuid: {donor_uuid}, entity_type: {donor['entity_type']}")

        self.update_index(donor)

        # Index all the descendants of this donor
        for descendant_uuid in descendant_uuids:
            # Retrieve the entity details
            descendant = self.get_entity(descendant_uuid)

            logger.info(f"indexer.index_tree() for donor descendant uuid: {descendant_uuid}, entity_type: {descendant['entity_type']}")

            self.update_index(descendant)

        msg = f"indexer.index_tree() finished executing for donor of uuid: {donor_uuid}"
        logger.info(msg)
        return msg

    def index_public_collection(self, uuid, reindex = False):
        logger.debug(f"Reindex public Collection with uuid: {uuid}")

        # The entity-api returns public collection with a list of connected public/published datasets, for either 
        # - a valid token but not in HuBMAP-Read group or 
        # - no token at all
        # Here we do NOT send over the token
        try:
            collection = self.get_public_collection(uuid)
        except requests.exceptions.RequestException as e:
            logger.exception(e)

            # Stop running
            msg = "indexer.index_public_collection() failed to get public collection of uuid: {uuid} via entity-api"
            logger.error(msg)
            sys.exit(msg)
  
        self.add_datasets_to_collection(collection)
        self.entity_keys_rename(collection)

        # Add additional calculated fields if any applies to Collection
        self.add_calculated_fields(collection)

        # write doc into indices
        for index in self.indices.keys():
            # each index should have a public index
            public_index = self.INDICES['indices'][index]['public']
            private_index = self.INDICES['indices'][index]['private']
            
            # Delete old doc for reindex
            if reindex:
                self.eswriter.delete_document(public_index, uuid)
                self.eswriter.delete_document(private_index, uuid)

            # Add the tranformed doc to the portal index
            json_data = ""

            # if the index has a transformer use that else do a now load
            if self.TRANSFORMERS.get(index):
                json_data = json.dumps(self.TRANSFORMERS[index].transform(collection))
            else:
                json_data = json.dumps(collection)

            self.eswriter.write_or_update_document(index_name=public_index, doc=json_data, uuid=uuid)
            self.eswriter.write_or_update_document(index_name=private_index, doc=json_data, uuid=uuid)


    # When indexing, Upload WILL NEVER BE PUBLIC
    def index_upload(self, uuid, reindex = False):
        logger.debug(f"Reindex Upload with uuid: {uuid}")

        # Only add uploads to the hm_consortium_entities index (private index of the default)
        default_private_index = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['private']

        # Delete old doc for reindex
        if reindex:
            self.eswriter.delete_document(default_private_index, uuid)

        # Retrieve the upload entity details
        upload = self.get_entity(uuid)

        self.add_datasets_to_upload(upload)
        self.entity_keys_rename(upload)

        # Add additional calculated fields if any applies to Upload
        self.add_calculated_fields(upload)

        # Only add doc to hm_consortium_entities index
        # Do NOT tranform the doc and add to other indices
        self.eswriter.write_or_update_document(index_name=default_private_index, doc=json.dumps(upload), uuid=uuid)


    # These calculated fields are not stored in neo4j but will be generated
    # and added to the ES
    def add_calculated_fields(self, entity):
        # Add index_version by parsing the VERSION file
        entity['index_version'] = self.index_version

        # Add display_subtype
        if entity['entity_type'] in ['Upload', 'Donor', 'Sample', 'Dataset']:
            entity['display_subtype'] = self.generate_display_subtype(entity)


    def reindex(self, uuid):
        try:
            # Retrieve the entity details
            entity = self.get_entity(uuid)
            
            # Check if entity is empty
            if bool(entity):
                logger.info(f"Executing reindex() for uuid: {uuid}, entity_type: {entity['entity_type']}")

                if entity['entity_type'] == 'Collection':
                    self.index_public_collection(uuid, reindex = True)
                elif entity['entity_type'] == 'Upload':
                    self.index_upload(uuid, reindex = True)
                else:
                    ancestor_uuids = []
                    descendant_uuids = []
                    previous_revision_uuids = []
                    next_revision_uuids = []

                    url = self.entity_api_url + "/ancestors/" + uuid + '?property=uuid'
                    ancestor_uuids_response = requests.get(url, headers = self.request_headers, verify = False)
                    if ancestor_uuids_response.status_code != 200:
                        msg = f"indexer.reindex() failed to get ancestor uuids via entity-api for target uuid: {uuid}"
                        logger.error(msg)
                        sys.exit(msg)
                    
                    ancestor_uuids = ancestor_uuids_response.json()

                    url = self.entity_api_url + "/descendants/" + uuid + '?property=uuid'
                    descendant_uuids_response = requests.get(url, headers = self.request_headers, verify = False)
                    if descendant_uuids_response.status_code != 200:
                        msg = f"indexer.reindex() failed to get descendant uuids via entity-api for target uuid: {uuid}"
                        logger.error()
                        sys.exit(msg)
                    
                    descendant_uuids = descendant_uuids_response.json()

                    # Only Dataset entities may have previous/next revisions
                    if entity['entity_type'] == 'Dataset':
                        url = self.entity_api_url + "/previous_revisions/" + uuid + '?property=uuid'
                        previous_revision_uuids_response = requests.get(url, headers = self.request_headers, verify = False)
                        if previous_revision_uuids_response.status_code != 200:
                            msg = f"indexer.reindex() failed to get previous revision uuids via entity-api for target uuid: {uuid}"
                            logger.error(msg)
                            sys.exit(msg)
                        
                        previous_revision_uuids = previous_revision_uuids_response.json()

                        url = self.entity_api_url + "/next_revisions/" + uuid + '?property=uuid'
                        next_revision_uuids_response = requests.get(url, headers = self.request_headers, verify = False)
                        if next_revision_uuids_response.status_code != 200:
                            msg = f"indexer.reindex() failed to get next revision uuids via entity-api for target uuid: {uuid}"
                            logger.error(msg)
                            sys.exit(msg)
                        
                        next_revision_uuids = next_revision_uuids_response.json()

                    # All uuids in the path excluding the entity itself
                    uuids = ancestor_uuids + descendant_uuids + previous_revision_uuids + next_revision_uuids

                    # Reindex the entity itself
                    logger.info(f"reindex() for uuid: {uuid}, entity_type: {entity['entity_type']}")
                    self.update_index(entity)

                    # Reindex the rest of the entities in the list
                    for entity_uuid in uuids:
                        # Retrieve the entity details
                        node = self.get_entity(entity_uuid)

                        logger.debug(f"entity_type: {node.get('entity_type', 'Unknown')}, uuid: {node.get('uuid', None)}")
                        
                        self.update_index(node)
                
                logger.info("################reindex() DONE######################")

                return "indexer.reindex() finished executing"
        except Exception:
            msg = "Exceptions during executing indexer.reindex()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # Used by app.py reindex_all_uuids() for Live reindex all 
    def delete(self, uuid):
        try:
            for index, _ in self.indices.items():
                # each index should have a public/private index
                public_index = self.INDICES['indices'][index]['public']
                self.eswriter.delete_document(public_index, uuid)

                private_index = self.INDICES['indices'][index]['private']
                if public_index != private_index:
                    self.eswriter.delete_document(private_index, uuid)
               
        except Exception:
            msg = "Exceptions during executing indexer.delete()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)


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
                        display_subtype = self.get_organ_description(entity['organ'])
                    else:
                        logger.error(f"Missing missing organ when specimen_type is set of Sample with uuid: {entity['uuid']}")
                else:
                    display_subtype = self.get_tissue_sample_description(entity['specimen_type'])
            else:
                logger.error(f"Missing specimen_type of Sample with uuid: {entity['uuid']}")
        elif entity_type == 'Dataset':
            if 'data_types' in entity:
                display_subtype = ','.join(entity['data_types'])
            else:
                logger.error(f"Missing data_types of Dataset with uuid: {entity['uuid']}")
        else:
            # Do nothing
            logger.error(f"Invalid entity_type: {entity_type}. Only generate display_subtype for Upload/Donor/Sample/Dataset")

        return display_subtype


    def get_organ_description(self, organ_code):
        definition_yaml_file = Path(__file__).absolute().parent.parent / 'search-schema/data/definitions/enums/organ_types.yaml'
        
        return self.load_definition_code_description(organ_code, definition_yaml_file)


    def get_tissue_sample_description(self, tissue_sample_code):
        definition_yaml_file = Path(__file__).absolute().parent.parent / 'search-schema/data/definitions/enums/tissue_sample_types.yaml'

        return self.load_definition_code_description(tissue_sample_code, definition_yaml_file)


    def load_definition_code_description(self, definition_code, definition_yaml_file):
        logger.debug(f"========definition_code: {definition_code}")

        with open(definition_yaml_file) as file:
            definition_dict = yaml.safe_load(file)

            logger.info(f"Definition yaml file {definition_yaml_file} loaded successfully")

            if definition_code in definition_dict:
                definition_desc = definition_dict[definition_code]['description']
            else:
                # Return the error message as description
                msg = f"Missing definition key {definition_code} in {definition_yaml_file}"

                logger.error(msg)

                # Use triple {{{}}}
                definition_desc = f"{{{definition_code}}}"

            logger.debug(f"========definition_desc: {definition_desc}")

            return definition_desc


    def generate_doc(self, entity, return_type):
        try:
            uuid = entity['uuid']

            if entity['entity_type'] != 'Upload':
                ancestors = []
                descendants = []
                ancestor_ids = []
                descendant_ids = []
                immediate_ancestors = []
                immediate_descendants = []

                # Do not call /ancestors/<id> directly to avoid performance/timeout issue
                url = self.entity_api_url + "/ancestors/" + uuid + "?property=uuid"
                ancestor_ids_response = requests.get(url, headers = self.request_headers, verify = False)
                if ancestor_ids_response.status_code != 200:
                    msg = f"indexer.generate_doc() failed to get ancestor uuids via entity-api for target uuid: {uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                ancestor_ids = ancestor_ids_response.json()

                for ancestor_uuid in ancestor_ids:
                    # Retrieve the entity details
                    ancestor_dict = self.get_entity(ancestor_uuid)

                    # Add to the list
                    ancestors.append(ancestor_dict)

                # Find the Donor?
                donor = None
                for a in ancestors:
                    if a['entity_type'] == 'Donor':
                        donor = copy.copy(a)
                        break

                url = self.entity_api_url + "/descendants/" + uuid + "?property=uuid"
                descendant_ids_response = requests.get(url, headers = self.request_headers, verify = False)
                if descendant_ids_response.status_code != 200:
                    msg = f"indexer.generate_doc() failed to get descendant uuids via entity-api for target uuid: {uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                descendant_ids = descendant_ids_response.json()

                for descendant_uuid in descendant_ids:
                    # Retrieve the entity details
                    descendant_dict = self.get_entity(descendant_uuid)

                    # Add to the list
                    descendants.append(descendant_dict)

                # Calls to /parents/<id> and /children/<id> have no performance/timeout concerns
                url = self.entity_api_url + "/parents/" + uuid
                parents_response = requests.get(url, headers = self.request_headers, verify = False)
                if parents_response.status_code != 200:
                    msg = f"indexer.generate_doc() failed to get parents via entity-api for uuid: {uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                immediate_ancestors = parents_response.json()

                url = self.entity_api_url + "/children/" + uuid
                children_response = requests.get(url, headers = self.request_headers, verify = False)
                if children_response.status_code != 200:
                    msg = f"indexer.generate_doc() failed to get children via entity-api for uuid: {uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                immediate_descendants = children_response.json()

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

                entity['origin_sample'] = copy.copy(entity) if ('specimen_type' in entity) and (entity['specimen_type'].lower() == 'organ') and ('organ' in entity) and (entity['organ'].strip() != '') else None

                if entity['origin_sample'] is None:
                    try:
                        # The origin_sample is the ancestor which `specimen_type` is "organ" and the `organ` code is set
                        entity['origin_sample'] = copy.copy(next(a for a in ancestors if ('specimen_type' in a) and (a['specimen_type'].lower() == 'organ') and ('organ' in a) and (a['organ'].strip() != '')))
                    except StopIteration:
                        entity['origin_sample'] = {}

                # Trying to understand here!!!
                if entity['entity_type'] == 'Dataset':
                    entity['source_sample'] = None

                    e = entity
                    
                    while entity['source_sample'] is None:
                        url = self.entity_api_url + "/parents/" + e['uuid']
                        parents_resp = requests.get(url, headers = self.request_headers, verify = False)
                        if parents_resp.status_code != 200:
                            msg = f"indexer.generate_doc() failed to get parents via entity-api for uuid: {e['uuid']}"
                            logger.error(msg)
                            sys.exit(msg)

                        parents = parents_resp.json()

                        try:
                            # Why?
                            if parents[0]['entity_type'] == 'Sample':
                                #entity['source_sample'] = parents[0]
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
            response = requests.get(url, headers = self.request_headers, verify = False)

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

    # Initialize AuthHelper (AuthHelper from HuBMAP commons package)
    # HuBMAP commons AuthHelper handles "MAuthorization" or "Authorization"
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

        properties_list = [
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

        for key in to_delete_keys:
            if key not in properties_list:
                entity.pop(key)
        
        entity.update(temp)

        # logger.debug("==================entity after renaming keys==================")
        # logger.debug(entity)


    def remove_specific_key_entry(self, obj, key_to_remove):
        if type(obj) == dict:
            if key_to_remove in obj.keys(): 
                obj.pop(key_to_remove)
            
            for key in obj.keys():
                self.remove_specific_key_entry(obj[key], key_to_remove)
        elif type(obj) == list:
            for e in obj:
                self.remove_specific_key_entry(e, key_to_remove)

    def update_index(self, node):
        try:
            org_node = copy.deepcopy(node)

            doc = self.generate_doc(node, 'json')

            # Handle Upload differently by only updating it in the hm_consortium_entities index
            if node['entity_type'] == 'Upload':
                target_index = 'hm_consortium_entities'

                # Delete old doc and write with new one
                self.eswriter.delete_document(target_index, node['uuid'])
                self.eswriter.write_or_update_document(index_name=target_index, doc=doc, uuid=node['uuid'])
            else:

                # delete entity from public indices
                for index in self.indices.keys():
                    public_index = self.INDICES['indices'][index]['public']
                    self.eswriter.delete_document(public_index, node['uuid'])

                # write entity into indices
                for index in self.indices.keys():
               
                    public_index = self.INDICES['indices'][index]['public']
                    private_index = self.INDICES['indices'][index]['private']

                    # check to see if the index has a transformer, default to None if not found
                    transformer = self.TRANSFORMERS.get(index, None)

                    if (self.entity_is_public(org_node)):
                        public_doc = self.generate_public_doc(node)

                        if transformer is not None:                     
                            public_transformed = transformer.transform(json.loads(public_doc))
                            public_transformed_doc = json.dumps(public_transformed)
                            target_doc = public_transformed_doc
                        else:
                            target_doc = public_doc
                    
                        self.eswriter.write_or_update_document(index_name=public_index, doc=target_doc, uuid=node['uuid'])

                    # add it to private
                    if transformer is not None:
                        private_transformed = transformer.transform(json.loads(doc))
                        target_doc = json.dumps(private_transformed)
                    else:
                        target_doc = doc
       
                    self.eswriter.write_or_update_document(index_name=private_index, doc=target_doc, uuid=node['uuid'])
        
        except Exception:
            msg = f"Exception encountered during executing indexer.update_index() for uuid: {org_node['uuid']}, entity_type: {org_node['entity_type']}"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)


    # Collection doesn't actually have this `data_access_level` property
    # This method is only applied to Donor/Sample/Dataset
    # For Dataset, if status=='Published', it goes into the public index
    # For Donor/Sample, `data`if any dataset down in the tree is 'Published', they should have `data_access_level` as public,
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
                logger.error(f"{node['entity_type']} of uuid: {node['uuid']} missing 'status' property, treat as not public, verify and set the status.")
        else:
            # In case 'data_access_level' not set
            if 'data_access_level' in node:
                if node['data_access_level'].lower() == self.ACCESS_LEVEL_PUBLIC:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(f"{node['entity_type']} of uuid: {node['uuid']} missing 'data_access_level' property, treat as not public, verify and set the data_access_level.")
        
        return is_public


    def add_datasets_to_collection(self, collection):
        datasets = []
        if 'datasets' in collection:
            for dataset in collection['datasets']:
                # Retrieve the entity details
                dataset = self.get_entity(dataset['uuid'])

                dataset_doc = self.generate_doc(dataset, 'dict')
                dataset_doc.pop('ancestors')
                dataset_doc.pop('ancestor_ids')
                dataset_doc.pop('descendants')
                dataset_doc.pop('descendant_ids')
                dataset_doc.pop('immediate_descendants')
                dataset_doc.pop('immediate_ancestors')
                dataset_doc.pop('donor')
                dataset_doc.pop('origin_sample')
                dataset_doc.pop('source_sample')

                datasets.append(dataset_doc)

        collection['datasets'] = datasets
    
    # Currently the handling is same as add_datasets_to_collection()
    def add_datasets_to_upload(self, upload):
        datasets = []
        if 'datasets' in upload:
            for dataset in upload['datasets']:
                # Retrieve the entity details
                dataset = self.get_entity(dataset['uuid'])

                dataset_doc = self.generate_doc(dataset, 'dict')
                dataset_doc.pop('ancestors')
                dataset_doc.pop('ancestor_ids')
                dataset_doc.pop('descendants')
                dataset_doc.pop('descendant_ids')
                dataset_doc.pop('immediate_descendants')
                dataset_doc.pop('immediate_ancestors')
                dataset_doc.pop('donor')
                dataset_doc.pop('origin_sample')
                dataset_doc.pop('source_sample')

                datasets.append(dataset_doc)

        upload['datasets'] = datasets


    def get_entity(self, uuid):
        url = self.entity_api_url + "/entities/" + uuid
        response = requests.get(url, headers = self.request_headers, verify = False)

        if response.status_code != 200:
            # See if this uuid is a public Collection instead before exiting
            try:
                entity_dict = self.get_public_collection(uuid)
            except requests.exceptions.RequestException as e:
                logger.exception(e)
                
                # Stop running
                msg = f"indexer.get_entity() failed to get entity via entity-api for uuid: {uuid}"
                logger.error(msg)
                sys.exit(msg)
        else:
            entity_dict = response.json()

        return entity_dict


    def get_public_collection(self, uuid):
        # The entity-api returns public collection with a list of connected public/published datasets, for either 
        # - a valid token but not in HuBMAP-Read group or 
        # - no token at all
        # Here we do NOT send over the token
        url = self.entity_api_url + "/collections/" + uuid
        response = requests.get(url, verify = False)

        if response.status_code != 200:
            msg = "indexer.get_collection() failed to get public collection of uuid: {uuid} via entity-api"
            logger.exception(msg)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            raise requests.exceptions.RequestException(response.text)
    
        collection_dict = response.json()

        return collection_dict


####################################################################################################
## Run indexer.py as script
####################################################################################################

# To be used by the full index to ensure the nexus token 
# belongs to HuBMAP-Data-Admin group
def user_belongs_to_data_admin_group(user_group_ids, data_admin_group_uuid):
    for group_id in user_group_ids:
        if group_id == data_admin_group_uuid:
            return True

    # By now, no match
    return False


# Running indexer.py as a script in command line
# This approach is different from the live reindex via HTTP request
# It'll delete all the existing indices and recreate then then index everything
if __name__ == "__main__":
    # Specify the absolute path of the instance folder and use the config file relative to the instance path
    app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), '../instance'), instance_relative_config=True)
    app.config.from_pyfile('app.cfg')

    INDICES = safe_load((Path(__file__).absolute().parent / '../instance/search-config.yaml').read_text())

    try:
        token = sys.argv[1]
    except IndexError as e:
        msg = "Missing admin group token argument"
        logger.exception(msg)
        sys.exit(msg)

    # Create an instance of the indexer
    indexer = Indexer(
        INDICES,
        app.config['APP_CLIENT_ID'],
        app.config['APP_CLIENT_SECRET'],
        token
    )

    auth_helper = indexer.init_auth_helper()

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
    if not user_belongs_to_data_admin_group(group_ids, app.config['GLOBUS_HUBMAP_DATA_ADMIN_GROUP_UUID']):
        msg = "The given token doesn't belong to the HuBMAP-Data-Admin group, access not granted"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    start = time.time()
    logger.info("############# Full index via script started #############")

    indexer.main()

    end = time.time()
    logger.info(f"############# Full index via script completed. Total time used: {end - start} seconds. #############")



