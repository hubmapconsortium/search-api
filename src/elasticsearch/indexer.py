from neo4j import TransactionError, CypherError
from libs.es_writer import ESWriter
import sys, json, time, concurrent.futures, traceback, copy
import requests
import configparser
import ast
import os

config = configparser.ConfigParser()
config.read('conf.ini')

class Indexer:

    def __init__(self, index_name, elasticsearch_url, entity_webservice_url):
        self.eswriter = ESWriter(elasticsearch_url)
        self.entity_webservice_url = entity_webservice_url
        self.index_name = index_name
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'neo4j-to-es-attributes.json'), 'r') as json_file:
            self.attr_map = json.load(json_file)
        

    def main(self):
        try:
            self.eswriter.remove_index(self.index_name)
            self.eswriter.create_index(self.index_name)
            donors = requests.get(self.entity_webservice_url + "/entities?entitytypes=Donor").json()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.submit(self.index_tree, donor) for donor in donors]
                for f in concurrent.futures.as_completed(results):
                    print(f.result())
            # for donor in donors:
            #     self.index_tree(donor)
        
        except Exception as e:
            print(e)

    def index_tree(self, donor):
        descendants = requests.get(self.entity_webservice_url + "/entities/descendants/" + donor.get('uuid', None)).json()
        for node in [donor] + descendants:
            print(node.get('hubmap_identifier', node.get('display_doi', None)))
            doc = self.generate_doc(node)
            self.eswriter.write_document(self.index_name, doc, node['uuid'])

        return f"Done."

    def reindex(self, uuid):
        print(f"Before /entities/uuid/{uuid} call")
        print(self.entity_webservice_url + "/entities/uuid/" + uuid)
        entity = requests.get(self.entity_webservice_url + "/entities/uuid/" + uuid).json()['entity']
        print(f"After /entities/uuid/{uuid} call")
        print(f"Before /entities/ancestors/{uuid} call")
        ancestors = requests.get(self.entity_webservice_url + "/entities/ancestors/" + uuid).json()
        print(f"After /entities/ancestors/{uuid} call")
        print(f"Before /entities/descendants/{uuid} call")
        descendants = requests.get(self.entity_webservice_url + "/entities/descendants/" + uuid).json()
        print(f"After /entities/descendants/{uuid} call")
        nodes = [entity] + ancestors + descendants

        for node in nodes:
            print(node.get('entitytype', 'Unknown Entitytype'), " ", node.get('hubmap_identifier', node.get('display_doi', None)))
            doc = self.generate_doc(node)
            self.eswriter.delete_document(self.index_name, node['uuid'])
            self.eswriter.write_document(self.index_name, doc, node['uuid'])
        
        return f"Done."

    def generate_doc(self, entity):
        try:
            ancestors = requests.get(self.entity_webservice_url + "/entities/ancestors/" + entity.get('uuid', None)).json()
            descendants = requests.get(self.entity_webservice_url + "/entities/descendants/" + entity.get('uuid', None)).json()

            donor = None
            for a in ancestors:
                if a['entitytype'] == 'Donor':
                    donor = copy.copy(a)
                    break
            #     if 'ingest_metadata' in a:
            #         a['ingest_metadata'] = str(a['ingest_metadata'])
            # for d in descendants:
            #     if 'ingest_metadata' in d:
            #         d['ingest_metadata'] = str(d['ingest_metadata'])
            # build json
            entity['ancestor_ids'] = [a.get('uuid', 'missing') for a in ancestors]
            entity['descendant_ids'] = [d.get('uuid', 'missing') for d in descendants]
            entity['ancestors'] = ancestors
            entity['descendants'] = descendants
            entity['access_group'] = self.access_group(entity)

            if entity['entitytype'] in ['Sample', 'Dataset']:
                entity['donor'] = donor
                entity['origin_sample'] = copy.copy(entity) if 'organ' in entity['metadata'] else None
                if entity['origin_sample'] is None:
                    entity['origin_sample'] = copy.copy(next(a for a in ancestors if 'organ' in a['metadata']))
                # e = entity
                # while entity['origin_sample'] is None:
                #     parents = requests.get(self.entity_webservice_url + "/entities/parents/" + e.get('uuid', None)).json()
                #     if parents[0]['entitytype'] == 'Donor':
                #         entity['origin_sample'] = e
                #     e = parents[0]
                # entity['origin_sample'] = requests.get(self.entity_webservice_url + "/entities/children/" + donor.get('uuid', None)).json()[0]
                if entity['entitytype'] == 'Dataset':
                    entity['source_sample'] = None
                    e = entity
                    while entity['source_sample'] is None:
                        parents = requests.get(self.entity_webservice_url + "/entities/parents/" + e.get('uuid', None)).json()
                        if parents[0]['entitytype'] == 'Sample':
                            entity['source_sample'] = parents
                        e = parents[0]

                    # move files to the root level
                    try:
                        entity['files'] = ast.literal_eval(entity['metadata']['ingest_metadata'])['files']
                    except KeyError:
                        print("There is either no files in ingest_metadata or no ingest_metdata in metadata. Skip.")

            self.entity_keys_rename(entity)

            try:
                entity['metadata'].pop('files')
            except KeyError:
                print("There is no files in metadata to pop")

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

            self.remove_specific_key_entry(entity, "other_metadata")

            return json.dumps(entity)

        except Exception as e:
            print("Exception in user code:")
            print('-'*60)
            traceback.print_exc(file=sys.stdout)
            print('-'*60)
    
    
    def entity_keys_rename(self, entity):
        to_delete_keys = []
        temp = {}
        for key in entity:
            to_delete_keys.append(key)
            if key in self.attr_map['ENTITY']:
                temp[self.attr_map['ENTITY'][key]['es_name']] = ast.literal_eval(entity[key]) if self.attr_map['ENTITY'][key]['is_json_stored_as_text'] else entity[key]
        for key in to_delete_keys:
            if key not in ['metadata', 'donor', 'origin_sample', 'source_sample', 'access_group', 'ancestor_ids', 'descendant_ids', 'ancestors', 'descendants', 'files']:
                entity.pop(key)
        entity.update(temp)
        
        temp = {}
        for key in entity['metadata']:
            if key in self.attr_map['METADATA']:
                temp[self.attr_map['METADATA'][key]['es_name']] = ast.literal_eval(entity['metadata'][key]) if self.attr_map['METADATA'][key]['is_json_stored_as_text'] else entity['metadata'][key]
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
            print(e)

if __name__ == '__main__':
    try:
        index_name = sys.argv[1]
    except IndexError as ie:
        index_name = input("Please enter index name (Warning: All documents in this index will be clear out first): ")
    
    start = time.time()
    indexer = Indexer(index_name, config['ELASTICSEARCH']['ELASTICSEARCH_DOMAIN_ENDPOINT'], config['ELASTICSEARCH']['ENTITY_WEBSERVICE_URL'])
    indexer.main()
    end = time.time()
    print(end - start)