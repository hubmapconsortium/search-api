import sys
import json
import time
import concurrent.futures
import copy
import collections
import requests
import configparser
import ast
import os
import logging
from datetime import datetime
from pathlib import Path
from flask import current_app as app

# Local modules
from libs.es_writer import ESWriter
from elasticsearch.addl_index_transformations.portal import transform

# HuBMAP commons
from hubmap_commons.hubmap_const import HubmapConst
from hubmap_commons.provenance import Provenance

config = configparser.ConfigParser()
config.read('conf.ini')

ORIGINAL_DOC_TYPE = ""
PORTAL_DOC_TYPE = ""

REPLICATION = 1
# Set logging level (default is warning)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')


class Indexer:
    def __init__(self, indices, elasticsearch_url, entity_api_url):
        global ORIGINAL_DOC_TYPE
        global PORTAL_DOC_TYPE
        try:
            self.logger = app.logger
            ORIGINAL_DOC_TYPE = app.config['ORIGINAL_DOC_TYPE']
            PORTAL_DOC_TYPE = app.config['PORTAL_DOC_TYPE']
            app_client_id = app.config['APP_CLIENT_ID']
            app_client_secret = app.config['APP_CLIENT_SECRET']
            uuid_api_url = app.config['UUID_API_URL']
        except:
            ORIGINAL_DOC_TYPE = config['CONSTANTS']['ORIGINAL_DOC_TYPE']
            PORTAL_DOC_TYPE = config['CONSTANTS']['PORTAL_DOC_TYPE']
            self.logger = logging.getLogger(__name__)
            fh = logging.FileHandler('log')
            fh.setLevel(logging.INFO)
            fh.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
            sh = logging.StreamHandler(stream=sys.stdout)
            sh.setLevel(logging.INFO)
            sh.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
            self.logger.addHandler(fh)
            self.logger.addHandler(sh)
            self.logger.setLevel(logging.INFO)
            # self.logger.addHandler(logging.StreamHandler())
            app_client_id = config['GLOBUS']['APP_CLIENT_ID']
            app_client_secret = config['GLOBUS']['APP_CLIENT_SECRET']
            uuid_api_url = (config['ELASTICSEARCH']
                                         ['UUID_API_URL'])
        self.report = {
            'success_cnt': 0,
            'fail_cnt': 0,
            'fail_uuids': set()
        }

        self.eswriter = ESWriter(elasticsearch_url)
        self.entity_api_url = entity_api_url
        self.provenance = Provenance(app_client_id, app_client_secret, uuid_api_url)
        try:
            self.indices = ast.literal_eval(indices)
        except:
            raise ValueError("There is problem of indices config.")
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'neo4j-to-es-attributes.json'), 'r') as json_file:
            self.attr_map = json.load(json_file)

    def main(self):
        try:
            # Create Indices #
            for index, _ in self.indices.items():
                self.eswriter.remove_index(index)
                self.eswriter.create_index(index)
            
            # Entities 
            response = requests.get(app.config['ENTITY_API_URL'] + "/Donor/entities?property=uuid")
            
            if response.status_code != 200:
                self.logger.error("indexer.main() failed to make a request to entity-api for entity class: Donor")
            
            donors = response.json()

            # Multi-thread
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.submit(self.index_tree, donor) for donor in donors]
                for f in concurrent.futures.as_completed(results):
                    self.logger.debug(f.result())
            # for debuging: comment out the Multi-thread above and commnet in Signle-thread below
            # Single-thread
            # for donor in donors:
            #     self.index_tree(donor)

            # Index collections separately
            self.index_collections("token")
        except Exception:
            self.logger.error("Exception in user code:")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)

    def index_tree(self, donor):
        # self.logger.info(f"Total threads count: {threading.active_count()}")

        self.logger.info(f"index_tree() for : {donor['uuid']}")

        response = requests.get(self.entity_api_url + "/descendants/" + uuid)

        if response.status_code != 200:
            self.logger.error("indexer.index_tree() failed to get descendants via entity-api for uuid: " + uuid)
        
        descendants = response.json()

        for node in ([donor] + descendants):
            # hubamp_identifier renamed to submission_id 
            # disploy_doi renamed to hubmap_id
            self.logger.debug(f"entity_clss: {node.get('entity_class', 'Unknown Entity class')} submission_id: {node.get('submission_id', None)} hubmap_id: {node.get('hubmap_id', None)}")
            
            self.report[node['entity_class']] = self.report.get(node['entity_class'], 0) + 1

            self.update_index(node)

        return "Done."

    def index_collections(self, token):
        IndexConfig = collections.namedtuple('IndexConfig', ['access_level', 'doc_type'])
        # write enitty into indices
        for index, configs in self.indices.items():
            configs = IndexConfig(*configs)
            if (configs.access_level == 'consortium' and configs.doc_type == 'original'):
                
                # ??????????????????????
                # Consortium Collections #
                rspn = requests.get(self.entity_api_url + "/Collection/entities", headers={"Authorization": f"Bearer {token}"})
            elif (configs.access_level == HubmapConst.ACCESS_LEVEL_PUBLIC and configs.doc_type == 'original'):
                # Public Collections #
                rspn = requests.get(self.entity_api_url + "/Collection/entities")
            else:
                continue

            if not rspn.ok:
                if rspn.status_code == 401:
                    raise ValueError("Token is not valid.")
                else:
                    raise Exception("Something wrong with entity-api.")

            hm_collections = rspn.json()

            for collection in hm_collections:
                self.add_datasets_to_collection(collection)
                self.entity_keys_rename(collection)
                # Use `entity_type` instead of `entity_class` explicitly for Collection
                # Otherwise, it won't get reindexed
                # Because we are not rename the Collection.entity_class to entity_type in json mapping
                collection.setdefault('entity_type', 'Collection')
                (self.eswriter
                     .write_or_update_document(index_name=index, doc=json.dumps(collection), uuid=collection['uuid']))

                prefix0, prefix1, _ = index.split("_")
                index = f"{prefix0}_{prefix1}_portal"
                transformed = json.dumps(transform(collection))
                (self.eswriter.write_or_update_document(index_name=index, doc=transformed, uuid=collection['uuid']))


    def reindex(self, uuid):
        

        try:
            response = requests.get(self.entity_api_url + "/entities/" + uuid)

            if response.status_code != 200:
                self.logger.error("indexer.reindex() failed to get entity via entity-api for uuid: " + uuid)
            
            entity = response.json()
            
            # Check if entity is empty
            if bool(entity):
                self.logger.info("reindex() for uuid: " + uuid + " entity_class: " + entity['entity_class'])

                ancestors_response = requests.get(self.entity_api_url + "/ancestors/" + uuid)
                if ancestors_response.status_code != 200:
                    self.logger.error("indexer.reindex() failed to get ancestors via entity-api for uuid: " + uuid)
                
                ancestors = ancestors_response.json()

                descendants_response = requests.get(self.entity_api_url + "/descendants/" + uuid)
                if descendants_response.status_code != 200:
                    self.logger.error("indexer.reindex() failed to get descendants via entity-api for uuid: " + uuid)
                
                descendants = descendants_response.json()

                # All nodes in the path including the entity itself
                nodes = [entity] + ancestors + descendants

                for node in nodes:
                    # hubmap_identifier renamed to submission_id
                    # display_doi renamed to hubmap_id
                    self.logger.debug(f"entity_clss: {node.get('entity_class', 'Unknown Entity class')} submission_id: {node.get('submission_id', None)} hubmap_id: {node.get('hubmap_id', None)}")
                    
                    self.logger.info("reindex(): About to update_index")
                    self.update_index(node)
                
                self.logger.info("################reindex() DONE######################")
                return f"Done."
            else:
                collection = {}
                #This uuid is a collection
                if collection != {}:
                    self.index_collection(collection)

                    self.logger.info("################DONE######################")
                    return f"Done."
                else:
                    self.logger.error(f"Cannot find uuid: {uuid}")
                    return f"Done."
        except Exception as e:
            self.logger.error("Exception in user code:")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)

    def delete(self, uuid):
        try:
            for index, _ in self.indices.items():
                self.eswriter.delete_document(index, uuid)
        except Exception:
            self.logger.error("Exception in user code:")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)

    def generate_doc(self, entity, return_type):
        try:
            uuid = entity['uuid']
            ancestors = []
            descendants = []
            ancestor_ids = []
            descendant_ids = []

            ancestors_response = requests.get(self.entity_api_url + "/ancestors/" + uuid)
            if ancestors_response.status_code != 200:
                self.logger.error("indexer.generate_doc() failed to get ancestors via entity-api for uuid: " + uuid)

            ancestors = ancestors_response.json()

            ancestor_ids_response = requests.get(self.entity_api_url + "/ancestors/" + uuid + "?property=uuid")
            if ancestor_ids_response.status_code != 200:
                self.logger.error("indexer.generate_doc() failed to get ancestors ids list via entity-api for uuid: " + uuid)

            ancestor_ids = ancestor_ids_response.json()

            descendants_response = requests.get(self.entity_api_url + "/descendants/" + uuid)
            if descendants_response.status_code != 200:
                self.logger.error("indexer.generate_doc() failed to get descendants via entity-api for uuid: " + uuid)

            descendants = descendants_response.json()

            descendant_ids_response = requests.get(self.entity_api_url + "/descendants/" + uuid + "?property=uuid")
            if descendant_ids_response.status_code != 200:
                self.logger.error("indexer.generate_doc() failed to get descendants ids list via entity-api for uuid: " + uuid)

            descendant_ids = descendant_ids_response.json()

            donor = None
            for a in ancestors:
                if a['entity_class'] == 'Donor':
                    donor = copy.copy(a)
                    break

            # build json
            entity['ancestor_ids'] = ancestor_ids
            entity['descendant_ids'] = descendant_ids

            entity['ancestors'] = ancestors
            entity['descendants'] = descendants
            # entity['access_group'] = self.access_group(entity)
            
            children_response = requests.get(self.entity_api_url + "/children/" + uuid)
            if children_response.status_code != 200:
                self.logger.error("indexer.generate_doc() failed to get children via entity-api for uuid: " + uuid)

            entity['immediate_descendants'] = children_response.json()

            parents_response = requests.get(self.entity_api_url + "/parents/" + uuid)
            if parents_response.status_code != 200:
                self.logger.error("indexer.generate_doc() failed to get parents via entity-api for uuid: " + uuid)

            entity['immediate_ancestors'] = parents_response.json()


            # Why?
            if entity['entity_class'] in ['Sample', 'Dataset']:
                entity['donor'] = donor
                # Add a new property
                entity['origin_sample'] = copy.copy(entity) if 'organ' in entity and entity['organ'].strip() != "" else None
                
                if entity['origin_sample'] is None:
                    try:
                        entity['origin_sample'] = copy.copy(next(a for a in ancestors if 'organ' in a and a['organ'].strip() != ""))
                    except StopIteration:
                        entity['origin_sample'] = {}

                # Trying to understand here!!!
                if entity['entity_class'] == 'Dataset':
                    entity['source_sample'] = None

                    e = entity
                    
                    while entity['source_sample'] is None:
                        parents_resp = requests.get(self.entity_api_url + "/parents/" + e['uuid'])
                        if parents_resp.status_code != 200:
                            self.logger.error("indexer.generate_doc() failed to get parents via entity-api for uuid: " + e['uuid'])
                        parents = parents_resp.json()

                        try:
                            # Why?
                            if parents[0]['entity_class'] == 'Sample':
                                entity['source_sample'] = parents

                            e = parents[0]
                        except IndexError:
                             entity['source_sample'] = {}

                    # move files to the root level
                    try:
                        entity['files'] = ast.literal_eval(entity['ingest_metadata'])['files']
                    except KeyError:
                        self.logger.debug("There are either no files in ingest_metadata or no ingest_metdata in metadata. Skip.")
                    except TypeError:
                        self.logger.debug("There are either no files in ingest_metadata or no ingest_metdata in metadata. Skip.")

            self.entity_keys_rename(entity)

            group = (self.provenance.get_group_by_identifier(entity['group_uuid']))
            entity['group_name'] = group['displayname']

            # timestamp and version
            entity['update_timestamp'] = int(round(time.time() * 1000))
            entity['update_timestamp_fmted'] = (datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            entity['index_version'] = ((Path(__file__).parent.parent / 'VERSION').read_text()).strip()

            try:
                entity['metadata'].pop('files')
            except KeyError:
                self.logger.debug("There are no files in metadata to pop")
            except AttributeError:
                self.logger.debug("There are no files in metadata to pop")

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

        except Exception as e:
            self.logger.error(f"Exception in generate_doc()")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)

    def generate_public_doc(self, entity):
        entity['descendants'] = list(filter(self.entity_is_public, entity['descendants']))
        entity['immediate_descendants'] = list(filter(self.entity_is_public, entity['immediate_descendants']))
        return json.dumps(entity)

    def entity_keys_rename(self, entity):
        self.logger.debug("==================entity before renaming keys==================")
        self.logger.debug(entity)

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

        self.logger.debug("==================entity after renaming keys==================")
        self.logger.debug(entity)
        

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
            if entity['entity_class'] == 'Dataset':
                if entity['status'] == 'Published' and entity['contains_human_genetic_sequences'] == False:
                    return HubmapConst.ACCESS_LEVEL_PUBLIC
                else:
                    return HubmapConst.ACCESS_LEVEL_CONSORTIUM
            else:
                return HubmapConst.ACCESS_LEVEL_CONSORTIUM
        
        except Exception as e:
            self.logger.error("Exception in user code:")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)

    def get_access_level(self, entity):
        try:
            entity_class = entity['entity_class'] if 'entity_class' in entity else entity['entity_type']

            if entity_class:
                if entity_class == HubmapConst.COLLECTION_TYPE_CODE:
                    if entity['data_access_level'] in HubmapConst.DATA_ACCESS_LEVEL_OPTIONS:
                        return entity['data_access_level']
                    else:
                        return HubmapConst.ACCESS_LEVEL_CONSORTIUM
                # Hard code instead of use commons constants for now.
                elif entity_class in ['Donor', 'Sample', 'Dataset']:
                    
                    dal = entity['data_access_level']

                    if dal in HubmapConst.DATA_ACCESS_LEVEL_OPTIONS:
                        return dal
                    else:
                        return HubmapConst.ACCESS_LEVEL_CONSORTIUM
                else:
                    raise ValueError("The type of entitiy is not Donor, Sample, Collection or Dataset")
        except KeyError as ke:
            self.logger.debug(f"Entity of uuid: {entity['uuid']} does not have 'data_access_level' attribute")
            return HubmapConst.ACCESS_LEVEL_CONSORTIUM
        except Exception:
            pass

    def test(self):
        try:
            donors = requests.get(self.entity_api_url + "/Donor/entities").json()
            # hubmap_identifier renamed to submission_id
            donors = [donor for donor in donors if donor['submission_id'] == 'TEST0086']
            fk_donors = []
            for _ in range(100):
                fk_donors.append(copy.copy(donors[0]))
            # Multi-thread
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.submit(self.index_tree, donor) for donor in fk_donors]
                for f in concurrent.futures.as_completed(results):
                    self.logger.debug(f.result())
            
            # Single-thread
            # for donor in fk_donors:
            #     self.index_tree(donor)

        except Exception as e:
            self.logger.error("Exception in user code:")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)

    def update_index(self, node):
        try:
            org_node = copy.deepcopy(node)

            # Do we realy need this?
            node.setdefault('type', 'entity')

            doc = self.generate_doc(node, 'json')
            transformed = json.dumps(transform(json.loads(doc)))
            if (transformed is None or transformed == 'null' or transformed == ""):
                self.logger.error(f"{node['uuid']} Document is empty")
                self.logger.error(f"Node: {node}")
                return

            result = None
            IndexConfig = collections.namedtuple('IndexConfig', ['access_level', 'doc_type'])
            # delete entity from published indices
            for index, configs in self.indices.items():
                configs = IndexConfig(*configs)
                if configs.access_level == HubmapConst.ACCESS_LEVEL_PUBLIC:
                    self.eswriter.delete_document(index, node['uuid'])

            # write enitty into indices
            for index, configs in self.indices.items():
                configs = IndexConfig(*configs)
                if (configs.access_level == HubmapConst.ACCESS_LEVEL_PUBLIC and self.entity_is_public(org_node)):
                    public_doc = self.generate_public_doc(node)
                    public_transformed = transform(json.loads(public_doc))
                    public_transformed_doc = json.dumps(public_transformed)
                    result = (self.eswriter.write_or_update_document(
                                index_name=index,
                                doc=(public_transformed_doc
                                     if configs.doc_type == PORTAL_DOC_TYPE
                                     else public_doc),
                                uuid=node['uuid']))
                elif configs.access_level == HubmapConst.ACCESS_LEVEL_CONSORTIUM:
                    result = (self.eswriter.write_or_update_document(
                                index_name=index,
                                doc=(transformed
                                     if configs.doc_type == PORTAL_DOC_TYPE
                                     else doc),
                                uuid=node['uuid']))
                if result:
                    self.report['success_cnt'] += 1
                else:
                    self.report['fail_cnt'] += 1
                    self.report['fail_uuids'].add(node['uuid'])
                result = None
        except KeyError:
            self.logger.error(f"""uuid: {org_node['uuid']}, entity_class: {org_node['entity_class']}, es_node_entity_class: {node['entity_class']}""")
            self.logger.exception("unexpceted exception")
        except Exception as e:
            self.report['fail_cnt'] +=1
            self.report['fail_uuids'].add(node['uuid'])

            self.logger.error(f"""Exception in user code, 
                        uuid: {org_node['uuid']}""")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)

    def entity_is_public(self, node):
        # Here the node properties have already been renamed
        if 'entity_type' in node:  # Tranformed Node
            return ((node.get('entity_type', '') == 'Dataset' and
                    node.get('status', '') == HubmapConst.DATASET_STATUS_PUBLISHED) or
                    (node.get('entity_type', '') != 'Dataset' and
                    self.get_access_level(node) == HubmapConst.ACCESS_LEVEL_PUBLIC))
        else:  # Original Node
            return ((node.get('entity_class', '') == 'Dataset' and
                    node.get('status', '') == HubmapConst.DATASET_STATUS_PUBLISHED) or
                    (node.get('entity_class', '') != 'Dataset' and
                    self.get_access_level(node) == HubmapConst.ACCESS_LEVEL_PUBLIC))

    def add_datasets_to_collection(self, collection):
        datasets = []
        for uuid in collection['dataset_uuids']:
            response = requests.get(self.entity_api_url + "/entities/" + uuid)
            if re.status_code != 200:
                self.logger.error("indexer.add_datasets_to_collection() failed to get entity via entity-api for uuid: " + uuid)

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


if __name__ == '__main__':
    try:
        env = sys.argv[1]
    except IndexError as ie:
        # index_name = input("Please enter index name (Warning: All documents in this index will be cleared out first): ")
        print("using default DEV enviorment")
        print("replications: 1")
    
    if env == 'STAGE' or env == 'PROD':
        REPLICATION = 1
    else:
        REPLICATION = 0
        
    start = time.time()
    indexer = Indexer(config['INDEX']['INDICES'], config['ELASTICSEARCH']['ELASTICSEARCH_DOMAIN_ENDPOINT'], config['ELASTICSEARCH']['entity_api_url'])
    indexer.main()
    end = time.time()
    indexer.logger.info(f"Total index time: {end - start} seconds")
    indexer.logger.info(f"Success node count: {indexer.report['success_cnt']}")
    indexer.report.pop('success_cnt')
    indexer.logger.info(f"Fail node count: {indexer.report['fail_cnt']}")
    indexer.report.pop('fail_cnt')
    indexer.logger.info(f"Fail uuids: {indexer.report['fail_uuids']}")
    indexer.report.pop('fail_uuids')
    for key, value in indexer.report.items():
        indexer.logger.info(f"key: {key}, value: {value}")

    # start = time.time()
    # indexer = Indexer('entities', config['ELASTICSEARCH']['ELASTICSEARCH_DOMAIN_ENDPOINT'], config['ELASTICSEARCH']['entity_api_url'])
    # indexer.test()
    # end = time.time()
    # logging.info(f"Total index time: {end - start} seconds")