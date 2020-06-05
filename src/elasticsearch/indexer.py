from neo4j import TransactionError, CypherError
from libs.es_writer import ESWriter
from addl_index_transformations.portal import transform
import sys, json, time, concurrent.futures, traceback, copy, threading
import requests
import configparser
import ast
import os
import logging

config = configparser.ConfigParser()
config.read('conf.ini')

# Set logging level (default is warning)
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',handlers=[logging.FileHandler('log'), logging.StreamHandler()])

class Indexer:

    def __init__(self, elasticsearch_url, entity_webservice_url):
        self.eswriter = ESWriter(elasticsearch_url)
        self.entity_webservice_url = entity_webservice_url
        self.indices = {
            "hm_public_entities": ('Open','original'),
            "hm_consortium_entities": ('All', 'original'),
            "portal_public_entities": ('Open', 'transformed'),
            "portal_consortium_entities": ('All', 'transformed')
            }
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'neo4j-to-es-attributes.json'), 'r') as json_file:
            self.attr_map = json.load(json_file)
        
    def main(self):
        try:
            for index, _ in self.indices.items():
                self.eswriter.remove_index(index)
                self.eswriter.create_index(index)
            donors = requests.get(self.entity_webservice_url + "/entities?entitytypes=Donor").json()
            # Multi-thread
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.submit(self.index_tree, donor) for donor in donors]
                for f in concurrent.futures.as_completed(results):
                    logging.info(f.result())
            # for debuging: comment out the Multi-thread above and commnet in Signle-thread below
            # Single-thread
            # for donor in donors:
            #     self.index_tree(donor)
        
        except Exception as e:
            logging.error("Exception in user code:")
            logging.error('-'*60)
            traceback.print_exc(file=sys.stdout)
            logging.error('-'*60)

    def index_tree(self, donor):
        # logging.info(f"Total threads count: {threading.active_count()}")
        descendants = requests.get(self.entity_webservice_url + "/entities/descendants/" + donor.get('uuid', None)).json()
        for node in [donor] + descendants:
            logging.info(node.get('hubmap_identifier', node.get('display_doi', None)))
            self.update_index(node)

        return f"Done."

    def reindex(self, uuid):
        entity = requests.get(self.entity_webservice_url + "/entities/uuid/" + uuid).json()['entity']
        ancestors = requests.get(self.entity_webservice_url + "/entities/ancestors/" + uuid).json()
        descendants = requests.get(self.entity_webservice_url + "/entities/descendants/" + uuid).json()
        nodes = [entity] + ancestors + descendants

        for node in nodes:
            logging.info(f"{node.get('entitytype', 'Unknown Entitytype')} {node.get('hubmap_identifier', node.get('display_doi', None))}")
            self.update_index(node)
        
        logging.info("################DONE######################")
        return f"Done."

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
            entity['access_group'] = self.access_group(entity)
            
            entity['immediate_descendants'] = requests.get(self.entity_webservice_url + "/entities/children/" + entity.get('uuid', None)).json()
            entity['immediate_ancestors'] = requests.get(self.entity_webservice_url + "/entities/parents/" + entity.get('uuid', None)).json()

            if entity['entitytype'] in ['Sample', 'Dataset']:
                entity['donor'] = donor
                entity['origin_sample'] = copy.copy(entity) if 'organ' in entity['metadata'] else None
                if entity['origin_sample'] is None:
                    try:
                        entity['origin_sample'] = copy.copy(next(a for a in ancestors if 'organ' in a['metadata']))
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
                        logging.info("There are either no files in ingest_metadata or no ingest_metdata in metadata. Skip.")
                    except TypeError:
                        logging.info("There are either no files in ingest_metadata or no ingest_metdata in metadata. Skip.")

            self.entity_keys_rename(entity)

            try:
                entity['metadata'].pop('files')
            except KeyError:
                logging.info("There are no files in metadata to pop")
            except AttributeError:
                logging.info("There are no files in metadata to pop")

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
            logging.error("Exception in user code:")
            logging.error('-'*60)
            traceback.print_exc(file=sys.stdout)
            logging.error('-'*60)
   
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
                    logging.warning(f"SyntaxError. Failed to eval the field {key} to python object. Value of entity['metadata'][key]: {entity['metadata'][key]}")
                    temp[self.attr_map['METADATA'][key]['es_name']] = entity['metadata'][key]
                except ValueError:
                    logging.warning(f"ValueError. Failed to eval the field {key} to python object. Value of entity['metadata'][key]: {entity['metadata'][key]}")
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
                    return 'Open'
                else:
                    return 'Readonly'
            else:
                return 'Readonly'
        
        except Exception as e:
            logging.error("Exception in user code:")
            logging.error('-'*60)
            traceback.print_exc(file=sys.stdout)
            logging.error('-'*60)

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
                    logging.info(f.result())
            
            # Single-thread
            # for donor in fk_donors:
            #     self.index_tree(donor)

        except Exception as e:
            logging.error("Exception in user code:")
            logging.error('-'*60)
            traceback.print_exc(file=sys.stdout)
            logging.error('-'*60)

    def update_index(self, node):
        org_node = copy.deepcopy(node)
        doc = self.generate_doc(node)
        transformed = json.dumps(transform(json.loads(doc)))

        for index, configs in self.indices.items():
            
            if configs[0] == 'Open' and self.access_group(org_node) == 'Open':
                self.eswriter.write_or_update_document(index, transformed if configs[1] == 'transformed' else doc, node['uuid'])

            self.eswriter.write_or_update_document(index, transformed if configs[1] == 'transformed' else doc, node['uuid'])

if __name__ == '__main__':
    # try:
    #     index_name = sys.argv[1]
    # except IndexError as ie:
    #     index_name = input("Please enter index name (Warning: All documents in this index will be cleared out first): ")
    
    start = time.time()
    indexer = Indexer(config['ELASTICSEARCH']['ELASTICSEARCH_DOMAIN_ENDPOINT'], config['ELASTICSEARCH']['ENTITY_WEBSERVICE_URL'])
    indexer.main()
    end = time.time()
    logging.info(f"Total index time: {end - start} seconds")

    # start = time.time()
    # indexer = Indexer('entities', config['ELASTICSEARCH']['ELASTICSEARCH_DOMAIN_ENDPOINT'], config['ELASTICSEARCH']['ENTITY_WEBSERVICE_URL'])
    # indexer.test()
    # end = time.time()
    # logging.info(f"Total index time: {end - start} seconds")