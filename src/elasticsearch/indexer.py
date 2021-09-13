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

# For reusing the app.cfg configuration when running indexer.py as script
from flask import Flask

# Local modules
from libs.es_writer import ESWriter
from elasticsearch.addl_index_transformations.portal import transform

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

    # Constructor method with instance variables to be passed in
    def __init__(self, indices, original_doc_type, portal_doc_type, elasticsearch_url, entity_api_url, app_client_id, app_client_secret, token):
        try:
            self.indices = ast.literal_eval(indices)
        except:
            raise ValueError("Invalid indices config")

        self.original_doc_type = original_doc_type
        self.portal_doc_type = portal_doc_type
        self.elasticsearch_url = elasticsearch_url
        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token

        auth_helper = self.init_auth_helper()
        self.request_headers = self.create_request_headers_for_auth(token)

        self.eswriter = ESWriter(elasticsearch_url)
        self.entity_api_url = entity_api_url

        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent.parent / 'VERSION').read_text()).strip()

        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'neo4j-to-es-attributes.json'), 'r') as json_file:
            self.attr_map = json.load(json_file)

    def main(self):
        try:
            # Settings and mappings definition for creating 
            # the original indices (hm_consortium_entities and hm_public_entities)
            original_index_config = {
                "settings": {
                    "index" : {
                        "mapping.total_fields.limit": 5000,
                        "query.default_field": 2048
                    }
                },
                "mappings": {
                    "date_detection": False
                }
            }

            # Settings and mappings definition for creating the 
            # portal indices (hm_consortium_portal and hm_public_portal) 
            # is specified in the yaml config file
            portal_index_config = safe_load((Path(__file__).absolute().parent / 'addl_index_transformations/portal/config.yaml').read_text())
            
            IndexConfig = collections.namedtuple('IndexConfig', ['access_level', 'doc_type'])

            # Delete and recreate target indices
            for index, configs in self.indices.items():
                configs = IndexConfig(*configs)

                self.eswriter.delete_index(index)

                # Use different settings/mappings for entities and portal indices on recreation
                if configs.doc_type == 'original':
                    self.eswriter.create_index(index, original_index_config)
                elif configs.doc_type == 'portal':
                    self.eswriter.create_index(index, portal_index_config)
                else:
                    msg = "indexer.main() failed to recreate indices due to invalid INDICES configuration"
                    logger.error(msg)
                    sys.exit(msg)
            
            # First, index public collections separately
            self.index_public_collections()

            # Next, index uploads separately
            self.index_uploads(self.token)

            # Then, get a list of donor dictionaries and index the tree from the root node - donor
            url = self.entity_api_url + "/donor/entities"
            response = requests.get(url, headers = self.request_headers, verify = False)
            
            if response.status_code != 200:
                msg = "indexer.main() failed to get all the Donors via entity-api"
                logger.error(msg)
                sys.exit(msg)
            
            donors = response.json()

            # Multi-thread
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.submit(self.index_tree, donor) for donor in donors]
                for f in concurrent.futures.as_completed(results):
                    logger.debug(f.result())
            
            # for debuging: comment out the Multi-thread above and commnet in Signle-thread below
            # Single-thread
            # for donor in donors:
            #     self.index_tree(donor)
        except Exception:
            msg = "Exception encountered during executing indexer.main()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def index_tree(self, donor):
        # logger.info(f"Total threads count: {threading.active_count()}")

        donor_uuid = donor['uuid']

        logger.info(f"Executing index_tree() for donor of uuid: {donor_uuid}")

        url = self.entity_api_url + "/descendants/" + donor_uuid
        response = requests.get(url, headers = self.request_headers, verify = False)

        if response.status_code != 200:
            msg = f"indexer.index_tree() failed to get descendants via entity-api for donor of uuid: {donor_uuid}"
            logger.error(msg)
            sys.exit(msg)
        
        descendants = response.json()

        for node in ([donor] + descendants):
            # hubamp_identifier renamed to submission_id 
            # disploy_doi renamed to hubmap_id
            logger.debug(f"entity_type: {node.get('entity_type', 'Unknown')} submission_id: {node.get('submission_id', None)} hubmap_id: {node.get('hubmap_id', None)}")
 
            self.update_index(node)

        msg = f"indexer.index_tree() finished executing for donor of uuid: {donor_uuid}"
        logger.info(msg)
        return msg

    def index_public_collections(self, reindex = False):
        # The entity-api only returns public collections, for either 
        # - a valid token in HuBMAP-Read group, 
        # - a valid token with no HuBMAP-Read group or 
        # - no token at all
        url = self.entity_api_url + "/collections"
        response = requests.get(url, verify = False)

        if response.status_code != 200:
            msg = "indexer.index_public_collections() failed to get public collections via entity-api"
            logger.error(msg)
            sys.exit(msg)
    
        collections_list = response.json()

        IndexConfig = collections.namedtuple('IndexConfig', ['access_level', 'doc_type'])

        # Write doc to indices
        for collection in collections_list:
            self.add_datasets_to_collection(collection)
            self.entity_keys_rename(collection)

            # Add additional caculated fields
            self.add_caculated_fields(collection)
   
            # write doc into indices
            for index, configs in self.indices.items():
                configs = IndexConfig(*configs)

                if configs.doc_type == 'original':
                    # Delete old doc for reindex
                    if reindex:
                        self.eswriter.delete_document(index, collection['uuid'])
                         
                    # Add public collection doc to the original index
                    self.eswriter.write_or_update_document(index_name=index, doc=json.dumps(collection), uuid=collection['uuid'])
                elif configs.doc_type == 'portal':
                    # Delete old doc for reindex
                    if reindex:
                        self.eswriter.delete_document(index, collection['uuid'])
                         
                    # Add the tranformed doc to the portal index
                    transformed = json.dumps(transform(collection))
                    self.eswriter.write_or_update_document(index_name=index, doc=transformed, uuid=collection['uuid'])
                else:
                    msg = "indexer.index_public_collections() failed to add doc to indices due to invalid INDICES configuration"
                    logger.error(msg)
                    sys.exit(msg)


    # When indexing Uploads WILL NEVER BE PUBLIC
    def index_uploads(self, token):
        IndexConfig = collections.namedtuple('IndexConfig', ['access_level', 'doc_type'])
        # write entity into indices
        for index, configs in self.indices.items():
            configs = IndexConfig(*configs)

            url = self.entity_api_url + "/upload/entities"

            # Only add uploads to the hm_consortium_entities index (original)
            if (configs.access_level == self.ACCESS_LEVEL_CONSORTIUM and configs.doc_type == 'original'):
                response = requests.get(url, headers = self.request_headers, verify = False)
            else:
                continue

            if response.status_code != 200:
                msg = "indexer.index_uploads() failed to get uploads via entity-api"
                logger.error(msg)
                sys.exit(msg)
        
            uploads_list = response.json()

            for upload in uploads_list:
                self.add_datasets_to_upload(upload)
                self.entity_keys_rename(upload)

                # Add additional caculated fields
                self.add_caculated_fields(upload)
       
                # Add doc to hm_consortium_entities index
                # Do NOT tranform the doc and add to hm_consortium_portal index
                self.eswriter.write_or_update_document(index_name=index, doc=json.dumps(upload), uuid=upload['uuid'])


    # These caculated fields are not stored in neo4j but will be generated
    # and added to the ES
    def add_caculated_fields(self, entity):
        # Add index_version by parsing the VERSION file
        entity['index_version'] = self.index_version

        # Add display_subtype
        if entity['entity_type'] in ['Upload', 'Donor', 'Sample', 'Dataset']:
            entity['display_subtype'] = self.generate_display_subtype(entity)


    # By design, reindex() doesn't work on Collection reindex
    # Use index_public_collections(reindex = True) for reindexing Collection
    def reindex(self, uuid):
        try:
            url = self.entity_api_url + "/entities/" + uuid
            response = requests.get(url, headers = self.request_headers, verify = False)

            if response.status_code != 200:
                msg = f"indexer.reindex() failed to get entity via entity-api for uuid: {uuid}"
                logger.error(msg)
                sys.exit(msg)
            
            entity = response.json()
            
            # Check if entity is empty
            if bool(entity):
                logger.info(f"reindex() for uuid: {uuid}, entity_type: {entity['entity_type']}")

                if entity['entity_type'] == 'Upload':
                    logger.debug(f"reindex Upload with uuid: {uuid}")
                    
                    self.update_index(entity)
                else:
                    url = self.entity_api_url + "/ancestors/" + uuid
                    ancestors_response = requests.get(url, headers = self.request_headers, verify = False)
                    if ancestors_response.status_code != 200:
                        msg = f"indexer.reindex() failed to get ancestors via entity-api for uuid: {uuid}"
                        logger.error(msg)
                        sys.exit(msg)
                    
                    ancestors = ancestors_response.json()

                    url = self.entity_api_url + "/descendants/" + uuid
                    descendants_response = requests.get(url, headers = self.request_headers, verify = False)
                    if descendants_response.status_code != 200:
                        msg = f"indexer.reindex() failed to get descendants via entity-api for uuid: {uuid}"
                        logger.error()
                        sys.exit(msg)
                    
                    descendants = descendants_response.json()

                    url = self.entity_api_url + "/previous_revisions/" + uuid
                    previous_revisions_response = requests.get(url, headers = self.request_headers, verify = False)
                    if previous_revisions_response.status_code != 200:
                        msg = f"indexer.reindex() failed to get previous revisions via entity-api for uuid: {uuid}"
                        logger.error(msg)
                        sys.exit(msg)
                    
                    previous_revisions = previous_revisions_response.json()

                    url = self.entity_api_url + "/next_revisions/" + uuid
                    next_revisions_response = requests.get(url, headers = self.request_headers, verify = False)
                    if next_revisions_response.status_code != 200:
                        msg = f"indexer.reindex() failed to get next revisions via entity-api for uuid: {uuid}"
                        logger.error(msg)
                        sys.exit(msg)
                    
                    next_revisions = next_revisions_response.json()

                    # All nodes in the path including the entity itself
                    nodes = [entity] + ancestors + descendants + previous_revisions + next_revisions

                    for node in nodes:
                        # hubmap_identifier renamed to submission_id
                        # display_doi renamed to hubmap_id
                        logger.debug(f"entity_type: {node.get('entity_type', 'Unknown')}, submission_id: {node.get('submission_id', None)}, hubmap_id: {node.get('hubmap_id', None)}")
                        
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
                self.eswriter.delete_document(index, uuid)
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

                url = self.entity_api_url + "/ancestors/" + uuid
                ancestors_response = requests.get(url, headers = self.request_headers, verify = False)
                if ancestors_response.status_code != 200:
                    msg = f"indexer.generate_doc() failed to get ancestors via entity-api for uuid: {uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                ancestors = ancestors_response.json()

                # Find the Donor?
                donor = None
                for a in ancestors:
                    if a['entity_type'] == 'Donor':
                        donor = copy.copy(a)
                        break

                url = self.entity_api_url + "/ancestors/" + uuid + "?property=uuid"
                ancestor_ids_response = requests.get(url, headers = self.request_headers, verify = False)
                if ancestor_ids_response.status_code != 200:
                    msg = f"indexer.generate_doc() failed to get ancestors ids list via entity-api for uuid: {uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                ancestor_ids = ancestor_ids_response.json()

                url = self.entity_api_url + "/descendants/" + uuid
                descendants_response = requests.get(url, headers = self.request_headers, verify = False)
                if descendants_response.status_code != 200:
                    msg = f"indexer.generate_doc() failed to get descendants via entity-api for uuid: {uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                descendants = descendants_response.json()

                url = self.entity_api_url + "/descendants/" + uuid + "?property=uuid"
                descendant_ids_response = requests.get(url, headers = self.request_headers, verify = False)
                if descendant_ids_response.status_code != 200:
                    msg = f"indexer.generate_doc() failed to get descendants ids list via entity-api for uuid: {uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                descendant_ids = descendant_ids_response.json()

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

            # Add additional caculated fields
            self.add_caculated_fields(entity)

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
                transformed = json.dumps(transform(json.loads(doc)))
                if (transformed is None or transformed == 'null' or transformed == ""):
                    logger.error(f"{node['uuid']} Document is empty")
                    logger.error(f"Node: {node}")
                    return

                result = None
                IndexConfig = collections.namedtuple('IndexConfig', ['access_level', 'doc_type'])
                # delete entity from published indices
                for index, configs in self.indices.items():
                    configs = IndexConfig(*configs)
                    if configs.access_level == self.ACCESS_LEVEL_PUBLIC:
                        self.eswriter.delete_document(index, node['uuid'])

                # write enitty into indices
                for index, configs in self.indices.items():
                    configs = IndexConfig(*configs)
                    if (configs.access_level == self.ACCESS_LEVEL_PUBLIC and self.entity_is_public(org_node)):
                        public_doc = self.generate_public_doc(node)
                        public_transformed = transform(json.loads(public_doc))
                        public_transformed_doc = json.dumps(public_transformed)
                        
                        target_doc = public_doc
                        if configs.doc_type == self.portal_doc_type:
                            target_doc = public_transformed_doc

                        self.eswriter.write_or_update_document(index_name=index, doc=target_doc, uuid=node['uuid'])
                    elif configs.access_level == self.ACCESS_LEVEL_CONSORTIUM:
                        target_doc = doc
                        if configs.doc_type == self.portal_doc_type:
                            target_doc = transformed

                        self.eswriter.write_or_update_document(index_name=index, doc=target_doc, uuid=node['uuid'])
        
        except Exception:
            msg = f"Exception encountered during executing indexer.update_index() for uuid: {org_node['uuid']}"
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
        # First get the detail of this collection
        collection_uuid = collection['uuid']
        url = self.entity_api_url + "/collections/" + collection_uuid
        response = requests.get(url, headers = self.request_headers, verify = False)
        if response.status_code != 200:
            msg = f"indexer.add_datasets_to_collection() failed to get collection detail via entity-api for collection uuid: {collection_uuid}"
            logger.error(msg)
            sys.exit(msg)

        collection_detail_dict = response.json()

        datasets = []
        if 'datasets' in collection_detail_dict:
            for dataset in collection_detail_dict['datasets']:
                dataset_uuid = dataset['uuid']
                url = self.entity_api_url + "/entities/" + dataset_uuid
                response = requests.get(url, headers = self.request_headers, verify = False)
                if response.status_code != 200:
                    msg = f"indexer.add_datasets_to_collection() failed to get dataset via entity-api for dataset uuid: {dataset_uuid} for collection uuid: {collection_uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                dataset = response.json()

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
    

    def add_datasets_to_upload(self, upload):
        # First get the detail of this upload
        upload_uuid = upload['uuid']
        url = self.entity_api_url + "/entities/" + upload_uuid
        response = requests.get(url, headers = self.request_headers, verify = False)
        if response.status_code != 200:
            msg = f"indexer.add_datasets_to_upload() failed to get upload detail via entity-api for upload uuid: {upload_uuid}"
            logger.error(msg)
            sys.exit(msg)

        upload_detail_dict = response.json()

        datasets = []
        if 'datasets' in upload_detail_dict:
            for dataset in upload_detail_dict['datasets']:
                dataset_uuid = dataset['uuid']
                url = self.entity_api_url + "/entities/" + dataset_uuid
                response = requests.get(url, headers = self.request_headers, verify = False)
                if response.status_code != 200:
                    msg = f"indexer.add_datasets_to_upload() failed to get dataset via entity-api for dataset uuid: {dataset_uuid} for upload uuid: {upload_uuid}"
                    logger.error(msg)
                    sys.exit(msg)

                dataset = response.json()

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


####################################################################################################
## Run indexer.py as script
####################################################################################################

# Get the user infomation dict based on the token
# To be used by the full index to ensure the nexus token 
# belongs to HuBMAP-Data-Admin group
def token_belongs_to_data_admin_group(token, data_admin_group_uuid):
    request_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + token
    }

    url='https://nexus.api.globusonline.org/groups?fields=id,name,description,group_type,has_subgroups,identity_set_properties&for_all_identities=false&include_identaaaaay_set_properties=false&my_statuses=active'
    
    response = requests.get(url, headers = request_headers)
    
    if response.status_code != 200:
        msg = (f"Unable to get groups information for token: {token}"
               f"{response.text}")

        logger.error(msg)
        sys.exit(msg)

    groups_info_list = response.json()

    for group_info in groups_info_list:
        if ('id' in group_info) and (group_info['id'] == data_admin_group_uuid):
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

    try:
        token = sys.argv[1]

        # Ensure the token belongs to the HuBMAP-Data-Admin group
        if not token_belongs_to_data_admin_group(token, app.config['GLOBUS_HUBMAP_DATA_ADMIN_GROUP_UUID']):
            msg = "The given token doesn't belong to the HuBMAP-Data-Admin group, access not granted"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            sys.exit(msg)
    except IndexError as e:
        msg = "Missing admin nexus token argument"
        logger.exception(msg)
        sys.exit(msg)

    # Create an instance of the indexer
    indexer = Indexer(
        app.config['INDICES'],
        app.config['ORIGINAL_DOC_TYPE'],
        app.config['PORTAL_DOC_TYPE'],
        app.config['ELASTICSEARCH_URL'],
        app.config['ENTITY_API_URL'],
        app.config['APP_CLIENT_ID'],
        app.config['APP_CLIENT_SECRET'],
        token
    )

    start = time.time()
    logger.info("############# Full index via script started #############")

    indexer.main()

    end = time.time()
    logger.info(f"############# Full index via script completed. Total time used: {end - start} seconds. #############")



