import sys
import json
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
from urllib3.exceptions import InsecureRequestWarning

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
    def __init__(self, indices, original_doc_type, portal_doc_type, elasticsearch_url, entity_api_url, app_client_id, app_client_secret):

        try:
            self.indices = ast.literal_eval(indices)
        except:
            raise ValueError("Invalid indices config")

        self.original_doc_type = original_doc_type
        self.portal_doc_type = portal_doc_type
        self.elasticsearch_url = elasticsearch_url
        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret

        auth_helper = self.init_auth_helper()
        self.request_headers = self.create_request_headers_for_auth(auth_helper.getProcessSecret())

        self.eswriter = ESWriter(elasticsearch_url)
        self.entity_api_url = entity_api_url

        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'neo4j-to-es-attributes.json'), 'r') as json_file:
            self.attr_map = json.load(json_file)


    def main(self):
        try:
            # Delete and recreate target indecies
            for index, _ in self.indices.items():
                self.eswriter.delete_index(index)
                self.eswriter.create_index(index)
            
            # Get a list of donor dictionaries 
            url = self.entity_api_url + "/donor/entities"
            response = requests.get(url, headers = self.request_headers, verify = False)
            
            if response.status_code != 200:
                logger.error("indexer.main() failed to make a request to entity-api for entity class: Donor")
            
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

            # Index collections separately
            self.index_collections()
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
            logger.error("indexer.index_tree() failed to get descendants via entity-api for donor of uuid: " + donor_uuid)
        
        descendants = response.json()

        for node in ([donor] + descendants):
            # hubamp_identifier renamed to submission_id 
            # disploy_doi renamed to hubmap_id
            logger.debug(f"entity_type: {node.get('entity_type', 'Unknown entity type')} submission_id: {node.get('submission_id', None)} hubmap_id: {node.get('hubmap_id', None)}")
 
            self.update_index(node)

        return "indexer.index_tree() finished executing"

    def index_collections(self):
        IndexConfig = collections.namedtuple('IndexConfig', ['access_level', 'doc_type'])
        # write entity into indices
        for index, configs in self.indices.items():
            configs = IndexConfig(*configs)

            url = self.entity_api_url + "/collections"

            if (configs.access_level == self.ACCESS_LEVEL_CONSORTIUM and configs.doc_type == 'original'):
                # Consortium Collections - with sending a token that has the right access permission
                response = requests.get(url, headers = self.request_headers, verify = False)
            elif (configs.access_level == self.ACCESS_LEVEL_PUBLIC and configs.doc_type == 'original'):
                # Public Collections - without sending token
                response = requests.get(url, verify = False)
            else:
                continue

            if response.status_code != 200:
                logger.error("indexer.index_collections() failed to get collections via entity-api")
        
            collections_list = response.json()

            for collection in collections_list:
                self.add_datasets_to_collection(collection)
                self.entity_keys_rename(collection)
                
                collection.setdefault('entity_type', 'Collection')
                self.eswriter.write_or_update_document(index_name=index, doc=json.dumps(collection), uuid=collection['uuid'])

                prefix0, prefix1, _ = index.split("_")
                index = f"{prefix0}_{prefix1}_portal"
                transformed = json.dumps(transform(collection))
                self.eswriter.write_or_update_document(index_name=index, doc=transformed, uuid=collection['uuid'])


    def reindex(self, uuid):
        try:
            url = self.entity_api_url + "/entities/" + uuid
            response = requests.get(url, headers = self.request_headers, verify = False)

            if response.status_code != 200:
                logger.error("indexer.reindex() failed to get entity via entity-api for uuid: " + uuid)
            
            entity = response.json()
            
            # Check if entity is empty
            if bool(entity):
                logger.info("reindex() for uuid: " + uuid + " entity_type: " + entity['entity_type'])

                url = self.entity_api_url + "/ancestors/" + uuid
                ancestors_response = requests.get(url, headers = self.request_headers, verify = False)
                if ancestors_response.status_code != 200:
                    logger.error("indexer.reindex() failed to get ancestors via entity-api for uuid: " + uuid)
                
                ancestors = ancestors_response.json()

                url = self.entity_api_url + "/descendants/" + uuid
                descendants_response = requests.get(url, headers = self.request_headers, verify = False)
                if descendants_response.status_code != 200:
                    logger.error("indexer.reindex() failed to get descendants via entity-api for uuid: " + uuid)
                
                descendants = descendants_response.json()

                # All nodes in the path including the entity itself
                nodes = [entity] + ancestors + descendants

                for node in nodes:
                    # hubmap_identifier renamed to submission_id
                    # display_doi renamed to hubmap_id
                    logger.debug(f"entity_type: {node.get('entity_type', 'Unknown entity type')} submission_id: {node.get('submission_id', None)} hubmap_id: {node.get('hubmap_id', None)}")
                    
                    logger.info("reindex(): About to update_index")
                    self.update_index(node)
                
                logger.info("################reindex() DONE######################")

                return "indexer.reindex() finished executing"
            else:
                collection = {}
                #This uuid is a collection
                if collection != {}:
                    self.index_collection(collection)

                    logger.info("################DONE######################")
                    return f"Done."
                else:
                    logger.error(f"Cannot find uuid: {uuid}")
                    return f"Done."
        except Exception:
            msg = "Exceptions during executing indexer.reindex()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def delete(self, uuid):
        try:
            for index, _ in self.indices.items():
                self.eswriter.delete_document(index, uuid)
        except Exception:
            msg = "Exceptions during executing indexer.delete()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def generate_doc(self, entity, return_type):
        try:
            uuid = entity['uuid']
            ancestors = []
            descendants = []
            ancestor_ids = []
            descendant_ids = []

            url = self.entity_api_url + "/ancestors/" + uuid
            ancestors_response = requests.get(url, headers = self.request_headers, verify = False)
            if ancestors_response.status_code != 200:
                logger.error("indexer.generate_doc() failed to get ancestors via entity-api for uuid: " + uuid)

            ancestors = ancestors_response.json()

            url = self.entity_api_url + "/ancestors/" + uuid + "?property=uuid"
            ancestor_ids_response = requests.get(url, headers = self.request_headers, verify = False)
            if ancestor_ids_response.status_code != 200:
                logger.error("indexer.generate_doc() failed to get ancestors ids list via entity-api for uuid: " + uuid)

            ancestor_ids = ancestor_ids_response.json()

            url = self.entity_api_url + "/descendants/" + uuid
            descendants_response = requests.get(url, headers = self.request_headers, verify = False)
            if descendants_response.status_code != 200:
                logger.error("indexer.generate_doc() failed to get descendants via entity-api for uuid: " + uuid)

            descendants = descendants_response.json()

            url = self.entity_api_url + "/descendants/" + uuid + "?property=uuid"
            descendant_ids_response = requests.get(url, headers = self.request_headers, verify = False)
            if descendant_ids_response.status_code != 200:
                logger.error("indexer.generate_doc() failed to get descendants ids list via entity-api for uuid: " + uuid)

            descendant_ids = descendant_ids_response.json()

            donor = None
            for a in ancestors:
                if a['entity_type'] == 'Donor':
                    donor = copy.copy(a)
                    break

            # build json
            entity['ancestor_ids'] = ancestor_ids
            entity['descendant_ids'] = descendant_ids

            entity['ancestors'] = ancestors
            entity['descendants'] = descendants
            # entity['access_group'] = self.access_group(entity)
            
            url = self.entity_api_url + "/children/" + uuid
            children_response = requests.get(url, headers = self.request_headers, verify = False)
            if children_response.status_code != 200:
                logger.error("indexer.generate_doc() failed to get children via entity-api for uuid: " + uuid)

            entity['immediate_descendants'] = children_response.json()
            
            url = self.entity_api_url + "/parents/" + uuid
            parents_response = requests.get(url, headers = self.request_headers, verify = False)
            if parents_response.status_code != 200:
                logger.error("indexer.generate_doc() failed to get parents via entity-api for uuid: " + uuid)

            entity['immediate_ancestors'] = parents_response.json()


            # Why?
            if entity['entity_type'] in ['Sample', 'Dataset']:
                # Add new properties
                entity['donor'] = donor
                entity['origin_sample'] = copy.copy(entity) if 'organ' in entity and entity['organ'].strip() != "" else None
                
                if entity['origin_sample'] is None:
                    try:
                        entity['origin_sample'] = copy.copy(next(a for a in ancestors if 'organ' in a and a['organ'].strip() != ""))
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
                            logger.error("indexer.generate_doc() failed to get parents via entity-api for uuid: " + e['uuid'])
                        parents = parents_resp.json()

                        try:
                            # Why?
                            if parents[0]['entity_type'] == 'Sample':
                                entity['source_sample'] = parents

                            e = parents[0]
                        except IndexError:
                             entity['source_sample'] = {}

                    # move files to the root level
                    try:
                        entity['files'] = ast.literal_eval(entity['ingest_metadata'])['files']
                    except (KeyError, TypeError):
                        logger.error("There are either no files in ingest_metadata or no ingest_metdata in metadata. Skip.")

            self.entity_keys_rename(entity)

                



            # Is group_uuid always set?
            # In case if group_name not set
            if ('group_uuid' in entity) and ('group_uuid' not in entity):
                group_uuid = entity['group_uuid']

                # Get the globus groups info based on the groups json file in commons package
                globus_groups_info = globus_groups.get_globus_groups_info()
                groups_by_id_dict = globus_groups_info['by_id']
                group_dict = groups_by_id_dict[group_uuid]

                # Add new property
                entity['group_name'] = group_dict['displayname']





            # timestamp and version
            entity['update_timestamp'] = int(round(time.time() * 1000))
            entity['update_timestamp_fmted'] = (datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            # Parse the VERSION number
            entity['index_version'] = ((Path(__file__).absolute().parent.parent.parent / 'VERSION').read_text()).strip()

            try:
                entity['metadata'].pop('files')
            except (KeyError, AttributeError):
                logger.error("There are no files in metadata to pop")

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

            return json.dumps(entity) if return_type == 'json' else entity
        except Exception:
            msg = "Exceptions during executing indexer.generate_doc()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def generate_public_doc(self, entity):
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
                temp[self.attr_map['ENTITY'][key]['es_name']] = ast.literal_eval(entity[key]) if self.attr_map['ENTITY'][key]['is_json_stored_as_text'] else entity[key]
        
        for key in to_delete_keys:
            if key not in ['metadata', 'donor', 'origin_sample', 'source_sample', 'access_group', 'ancestor_ids', 'descendant_ids', 'ancestors', 'descendants', 'files', 'immediate_ancestors', 'immediate_descendants', 'datasets']:
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

    def access_group(self, entity):
        try:
            if entity['entity_type'] == 'Dataset':
                if entity['status'] == 'Published' and entity['contains_human_genetic_sequences'] == False:
                    return self.ACCESS_LEVEL_PUBLIC
                else:
                    return self.ACCESS_LEVEL_CONSORTIUM
            else:
                return self.ACCESS_LEVEL_CONSORTIUM
        
        except Exception:
            msg = "Exception encountered during executing indexer.access_group()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def get_access_level(self, entity):
        try:
            entity_type = entity['entity_type']

            if entity_type == 'Collection':
                if entity['data_access_level'] in [self.ACCESS_LEVEL_PUBLIC, self.ACCESS_LEVEL_CONSORTIUM]:
                    return entity['data_access_level']
                else:
                    return self.ACCESS_LEVEL_CONSORTIUM
            # Hard code instead of use commons constants for now.
            elif entity_type in ['Donor', 'Sample', 'Dataset']:
                
                dal = entity['data_access_level']

                if dal in [self.ACCESS_LEVEL_PUBLIC, self.ACCESS_LEVEL_CONSORTIUM]:
                    return dal
                else:
                    return self.ACCESS_LEVEL_CONSORTIUM
            else:
                raise ValueError("The type of entitiy is not Donor, Sample, Collection or Dataset")
        except KeyError as ke:
            logger.error(f"Entity of uuid: {entity['uuid']} does not have 'data_access_level' attribute")
            return self.ACCESS_LEVEL_CONSORTIUM
        except Exception:
            pass


    def update_index(self, node):
        try:
            org_node = copy.deepcopy(node)

            # Do we realy need this?
            node.setdefault('type', 'entity')

            doc = self.generate_doc(node, 'json')
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
            msg = "Exception encountered during executing indexer.update_index() for uuid: " + org_node['uuid']
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)


    # Collection doesn't actually have this `data_access_level` property
    # This method is only applied to Donor/Sample/Dataset
    def entity_is_public(self, node):
        is_public = False
        
        if node['entity_type'] == 'Dataset':
            if ('status' in node) and (node['status'].lower() == self.DATASET_STATUS_PUBLISHED):
                is_public = True
        else:
            if self.get_access_level(node) == self.ACCESS_LEVEL_PUBLIC:
                is_public = True

        return is_public

    def add_datasets_to_collection(self, collection):
        # First get the detail of this collection
        collection_uuid = collection['uuid']
        url = self.entity_api_url + "/collections/" + collection_uuid
        response = requests.get(url, headers = self.request_headers, verify = False)
        if response.status_code != 200:
            logger.error("indexer.add_datasets_to_collection() failed to get collection detail via entity-api for collection uuid: " + collection_uuid)

        collection_detail_dict = response.json()

        datasets = []
        if 'datasets' in collection_detail_dict:
            for dataset in collection_detail_dict['datasets']:
                dataset_uuid = dataset['uuid']
                url = self.entity_api_url + "/entities/" + dataset_uuid
                response = requests.get(url, headers = self.request_headers, verify = False)
                if response.status_code != 200:
                    logger.info("Target collection uuid: " + collection_uuid)
                    logger.error("indexer.add_datasets_to_collection() failed to get dataset via entity-api for dataset uuid: " + dataset_uuid)

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


####################################################################################################
## Run indexer.py as script
####################################################################################################

# Running indexer.py as a script in command line
# This approach is different from the live reindex via HTTP request
# It'll delete all the existing indices and recreate then then index everything
if __name__ == "__main__":
    # Specify the absolute path of the instance folder and use the config file relative to the instance path
    app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), '../instance'), instance_relative_config=True)
    app.config.from_pyfile('app.cfg')

    # Create an instance of the indexer
    indexer = Indexer(
        app.config['INDICES'],
        app.config['ORIGINAL_DOC_TYPE'],
        app.config['PORTAL_DOC_TYPE'],
        app.config['ELASTICSEARCH_URL'],
        app.config['ENTITY_API_URL'],
        app.config['APP_CLIENT_ID'],
        app.config['APP_CLIENT_SECRET']
    )

    start = time.time()
    logger.info("############# Full index via script started #############")

    indexer.main()

    end = time.time()
    logger.info(f"############# Full index via script completed. Total time used: {end - start} seconds. #############")
