from libs.es_writer import ESWriter
from elasticsearch.addl_index_transformations.portal import transform
import sys, json, time, concurrent.futures, copy
import collections
import requests
import configparser
import ast
import os
import logging
from flask import current_app as app
from hubmap_commons.hubmap_const import HubmapConst

config = configparser.ConfigParser()
config.read('conf.ini')

ORIGINAL_DOC_TYPE = ""
PORTAL_DOC_TYPE = ""

REPLICATION = 1
# Set logging level (default is warning)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')


class Indexer:
    def __init__(self, indices, elasticsearch_url, entity_webservice_url):
        global ORIGINAL_DOC_TYPE
        global PORTAL_DOC_TYPE
        try:
            self.logger = app.logger
            ORIGINAL_DOC_TYPE = app.config['ORIGINAL_DOC_TYPE']
            PORTAL_DOC_TYPE = app.config['PORTAL_DOC_TYPE']
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
        self.report = {
            'success_cnt': 0,
            'fail_cnt': 0,
            'fail_uuids': set()
        }
        self.eswriter = ESWriter(elasticsearch_url)
        self.entity_webservice_url = entity_webservice_url
        try:
            self.indices = ast.literal_eval(indices)
        except:
            raise ValueError("There is problem of indices config.")
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'neo4j-to-es-attributes.json'), 'r') as json_file:
            self.attr_map = json.load(json_file)

    def main(self):
        try:
            #### Create Indices ####
            for index, _ in self.indices.items():
                self.eswriter.remove_index(index)
                self.eswriter.create_index(index)
            #### Entities ####
            donors = requests.get(self.entity_webservice_url + "/entities?entitytypes=Donor").json()
            # Multi-thread
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.submit(self.index_tree, donor) for donor in donors]
                for f in concurrent.futures.as_completed(results):
                    self.logger.debug(f.result())
            # for debuging: comment out the Multi-thread above and commnet in Signle-thread below
            # Single-thread
            # for donor in donors:
            #     self.index_tree(donor)
            #### Collections ####
            # collections = requests.get(self.entity_webservice_url + "/collections").json()
            # for collection in collections:
            #     self.index_collection(collections)
        
        except Exception as e:
            self.logger.error("Exception in user code:")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)

    def index_tree(self, donor):
        # self.logger.info(f"Total threads count: {threading.active_count()}")
        descendants = requests.get(self.entity_webservice_url + "/entities/descendants/" + donor.get('uuid', None)).json()
        for node in [donor] + descendants:
            self.logger.debug(node.get('hubmap_identifier', node.get('display_doi', None)))
            self.report[node['entitytype']] = self.report.get(node['entitytype'], 0) + 1
            self.update_index(node)

        return f"Done."

    def index_collection(self, collection):
        access_level = get_access_level(collection)
        if access_level == HubmapConst.ACCESS_LEVEL_PUBLIC:
            index = 'hm_public_entities'
            doc = json.dumps(collection)
            result = self.eswriter.write_or_update_document(index_name=index, type_='collection', doc=doc, uuid=collection['uuid'])
        elif access_level == HubmapConst.ACCESS_LEVEL_CONSORTIUM:
            index = 'hm_consortium_entities'
            doc = json.dumps(collection)
            result = self.eswriter.write_or_update_document(index_name=index, type_='collection', doc=doc, uuid=collection['uuid'])

    def reindex(self, uuid):
        try:
            entity = requests.get(self.entity_webservice_url + "/entities/uuid/" + uuid).json()['entity']
            # This uuid is a entity
            if entity != {}:
                ancestors = requests.get(self.entity_webservice_url + "/entities/ancestors/" + uuid).json()
                descendants = requests.get(self.entity_webservice_url + "/entities/descendants/" + uuid).json()
                nodes = [entity] + ancestors + descendants

                for node in nodes:
                    self.logger.debug(f"{node.get('entitytype', 'Unknown Entitytype')} {node.get('hubmap_identifier', node.get('display_doi', None))}")
                    self.update_index(node)
                
                self.logger.info("################DONE######################")
                return f"Done."
            else:
                # collection = requests.get(self.entity_webservice_url + "/collections/" + uuid).json()
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

    def generate_doc(self, entity):
        '''
            entity_keys_rename will change the entity inplace
        '''
        try:
            ancestors = requests.get(self.entity_webservice_url + "/entities/ancestors/" + entity.get('uuid', None)).json()
            descendants = requests.get(self.entity_webservice_url + "/entities/descendants/" + entity.get('uuid', None)).json()

            donor = None
            for a in ancestors:
                if a['entitytype'] == 'Donor':
                    donor = copy.copy(a)
                    break

            # build json
            entity['ancestor_ids'] = [a.get('uuid', 'missing') for a in ancestors]
            entity['descendant_ids'] = [d.get('uuid', 'missing') for d in descendants]
            entity['ancestors'] = ancestors
            entity['descendants'] = descendants
            # entity['access_group'] = self.access_group(entity)
            
            entity['immediate_descendants'] = requests.get(self.entity_webservice_url + "/entities/children/" + entity.get('uuid', None)).json()
            entity['immediate_ancestors'] = requests.get(self.entity_webservice_url + "/entities/parents/" + entity.get('uuid', None)).json()

            if entity['entitytype'] in ['Sample', 'Dataset']:
                entity['donor'] = donor
                entity['origin_sample'] = copy.copy(entity) if 'organ' in entity['metadata'] and entity['metadata']['organ'].strip() != "" else None
                if entity['origin_sample'] is None:
                    try:
                        entity['origin_sample'] = copy.copy(next(a for a in ancestors if 'organ' in a['metadata'] and a['metadata']['organ'].strip() != ""))
                    except StopIteration:
                        entity['origin_sample'] = {}

                if entity['entitytype'] == 'Dataset':
                    entity['source_sample'] = None
                    e = entity
                    while entity['source_sample'] is None:
                        parents = requests.get(self.entity_webservice_url + "/entities/parents/" + e.get('uuid', None)).json()
                        try:
                            if parents[0]['entitytype'] == 'Sample':
                                entity['source_sample'] = parents
                            e = parents[0]
                        except IndexError:
                             entity['source_sample'] = {}

                    # move files to the root level
                    try:
                        entity['files'] = ast.literal_eval(entity['metadata']['ingest_metadata'])['files']
                    except KeyError:
                        self.logger.debug("There are either no files in ingest_metadata or no ingest_metdata in metadata. Skip.")
                    except TypeError:
                        self.logger.debug("There are either no files in ingest_metadata or no ingest_metdata in metadata. Skip.")

            self.entity_keys_rename(entity)

            try:
                entity['metadata'].pop('files')
            except KeyError:
                self.logger.debug("There are no files in metadata to pop")
            except AttributeError:
                self.logger.debug("There are no files in metadata to pop")

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
                for id in entity.get('immediate_descendants', None):
                    self.entity_keys_rename(id)
            if entity.get('immediate_ancestors', None):
                for ia in entity.get('immediate_ancestors', None):
                    self.entity_keys_rename(ia)

            self.remove_specific_key_entry(entity, "other_metadata")

            return json.dumps(entity)

        except Exception as e:
            self.logger.error(f"Exception in generate_doc()")

    def entity_keys_rename(self, entity):
        to_delete_keys = []
        temp = {}
        for key in entity:
            to_delete_keys.append(key)
            if key in self.attr_map['ENTITY']:
                temp[self.attr_map['ENTITY'][key]['es_name']] = ast.literal_eval(entity[key]) if self.attr_map['ENTITY'][key]['is_json_stored_as_text'] else entity[key]
        for key in to_delete_keys:
            if key not in ['metadata', 'donor', 'origin_sample', 'source_sample', 'access_group', 'ancestor_ids', 'descendant_ids', 'ancestors', 'descendants', 'files', 'immediate_ancestors', 'immediate_descendants']:
                entity.pop(key)
        entity.update(temp)
        
        temp = {}
        for key in entity['metadata']:
            if key in self.attr_map['METADATA']:
                try:
                    temp[self.attr_map['METADATA'][key]['es_name']] = ast.literal_eval(entity['metadata'][key]) if self.attr_map['METADATA'][key]['is_json_stored_as_text'] else entity['metadata'][key]
                except SyntaxError:
                    self.logger.warning(f"SyntaxError. Failed to eval the field {key} to python object. Value of entity['metadata'][key]: {entity['metadata'][key]}")
                    temp[self.attr_map['METADATA'][key]['es_name']] = entity['metadata'][key]
                except ValueError:
                    self.logger.warning(f"ValueError. Failed to eval the field {key} to python object. Value of entity['metadata'][key]: {entity['metadata'][key]}")
                    temp[self.attr_map['METADATA'][key]['es_name']] = entity['metadata'][key]
        entity.pop('metadata')
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

    def access_group(self, entity):
        try:
            if entity['entitytype'] == 'Dataset':
                if entity['metadata']['status'] == 'Published' and entity['metadata']['phi'].lower() == 'no':
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
            if entity['entitytype'] == HubmapConst.COLLECTION_TYPE_CODE:
                if entity['data_access_level'] in HubmapConst.DATA_ACCESS_LEVEL_OPTIONS:
                    return entity['data_access_level']
                else:
                    return HubmapConst.ACCESS_LEVEL_CONSORTIUM
            elif entity['entitytype'] in [HubmapConst.DONOR_TYPE_CODE,\
                                        HubmapConst.SAMPLE_TYPE_CODE,\
                                        HubmapConst.DATASET_TYPE_CODE]:
                if entity['metadata']['data_access_level'] in HubmapConst.DATA_ACCESS_LEVEL_OPTIONS:
                    return entity['metadata']['data_access_level']
                else:
                    return HubmapConst.ACCESS_LEVEL_CONSORTIUM
            else:
                raise ValueError("The type of entitiy is not Donor, Sample, Collection or Dataset")
        except KeyError as ke:
            self.logger.debug(f"entity uuid: {entity['uuid']} does not have data_access_level attribute")
            return HubmapConst.ACCESS_LEVEL_CONSORTIUM
        except Exception:
            pass

    def test(self):
        try:
            donors = requests.get(self.entity_webservice_url + "/entities?entitytypes=Donor").json()
            donors = [donor for donor in donors if donor['hubmap_identifier'] == 'TEST0086']
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
            doc = self.generate_doc(node)
            transformed = json.dumps(transform(json.loads(doc)))
            if transformed is None or transformed == 'null' or transformed == "":
                self.logger.error(f"{node['uuid']} Document is empty")
                self.logger.error(f"Node: {node}")
                return

            result = None
            IndexConfig = collections.namedtuple('IndexConfig', ['access_level', 'doc_type'])
            for index, configs in self.indices.items():
                configs = IndexConfig(*configs)
                if configs.access_level == HubmapConst.ACCESS_LEVEL_PUBLIC and self.get_access_level(org_node) == HubmapConst.ACCESS_LEVEL_PUBLIC:
                    result = self.eswriter.write_or_update_document(index_name=index, doc=transformed if configs.doc_type == PORTAL_DOC_TYPE else doc, uuid=node['uuid'])
                elif configs.access_level == HubmapConst.ACCESS_LEVEL_CONSORTIUM:
                    result = self.eswriter.write_or_update_document(index_name=index, doc=transformed if configs.doc_type == PORTAL_DOC_TYPE else doc, uuid=node['uuid'])
                if result == True:
                    self.report['success_cnt'] += 1
                elif result == False:
                    self.report['fail_cnt'] +=1
                    self.report['fail_uuids'].add(node['uuid'])
                result = None
        except KeyError:
            self.logger.error(f"uuid: {org_node['uuid']}, entity_type: {org_node['entitytype']}, es_node_entity_type: {node['entity_type']}")
            self.logger.exception("unexpceted exception")
        except Exception as e:
            self.report['fail_cnt'] +=1
            self.report['fail_uuids'].add(node['uuid'])
            self.logger.error(f"Exception in user code, uuid: {org_node['uuid']}")
            self.logger.error('-'*60)
            self.logger.exception("unexpected exception")
            self.logger.error('-'*60)


if __name__ == '__main__':
    try:
        env = sys.argv[1]
    except IndexError as ie:
        # index_name = input("Please enter index name (Warning: All documents in this index will be cleared out first): ")
        print("using default DEV enviorment")
        print("replications: 1")
    
    if env == 'STAGE' or env == 'PROD':
        REPLICATION = 3
    else:
        REPLICATION = 1
        
    start = time.time()
    indexer = Indexer(config['INDEX']['INDICES'], config['ELASTICSEARCH']['ELASTICSEARCH_DOMAIN_ENDPOINT'], config['ELASTICSEARCH']['ENTITY_WEBSERVICE_URL'])
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
    # indexer = Indexer('entities', config['ELASTICSEARCH']['ELASTICSEARCH_DOMAIN_ENDPOINT'], config['ELASTICSEARCH']['ENTITY_WEBSERVICE_URL'])
    # indexer.test()
    # end = time.time()
    # logging.info(f"Total index time: {end - start} seconds")