from neo4j import TransactionError, CypherError
from src.libs.db_reader import DBReader
from src.libs.es_writer import ESWriter
import sys, json, time, concurrent.futures
import conf

class Main:

    def __init__(self, index_name):
        self.dbreader = DBReader(conf.NEO4J_CONF)
        self.eswriter = ESWriter(conf.ELASTICSEARCH_CONF)
        self.index_name = index_name

    def main(self):
        try:
            self.eswriter.remove_index(self.index_name)
            donors = self.dbreader.get_all_donors()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.submit(self.index_tree, donor) for donor in donors]
                for f in concurrent.futures.as_completed(results):
                    print(f.result())
            # for donor in donors:
            #     self.index_tree(donor)
        
        except Exception as e:
            print(e)
        else:
            self.dbreader.driver.close()

    def index_tree(self, donor):
        descendants = self.dbreader.get_all_descendants(donor.get('uuid', None))
        for node in [donor] + descendants:
            print(node.get('hubmap_identifier', None))
            doc = self.generate_doc(node)
            self.eswriter.wrtire_document(self.index_name, doc)
        return f"Done. {donor.get('hubmap_identifier', 'hubmap_identifier missing')}"

    def reindex(self, uuid):
        entity = self.dbreader.get_entity(uuid)
        acenstors = self.dbreader.get_all_ancenstors(uuid)
        descendants = self.dbreader.get_all_descendants(uuid)
        nodes = [entity] + acenstors + descendants

        for node in nodes:
            print(node.get('hubmap_identifier', None))
            doc = self.generate_doc(node)
            self.eswriter.wrtire_document(self.index_name, doc)
        
        return f"Done."

    def generate_doc(self, entity):
        try:
            ancenstors = self.dbreader.get_all_ancenstors(entity.get('uuid', None))
            descentdants = self.dbreader.get_all_descendants(entity.get('uuid', None))
            # build json
            entity['ancenstor_ids'] = [a.get('uuid', 'missing') for a in ancenstors]
            entity['descentdant_ids'] = [d.get('uuid', 'missing') for d in descentdants]
            entity['ancenstors'] = ancenstors
            entity['descentdants'] = descentdants
            entity['access_group'] = self.access_group(entity)
        
            return json.dumps(entity)

        except Exception as e:
            print(e)
    
    def access_group(self, entity):
        try:
            if entity['entitytype'] == 'Dataset':
                if entity['status'] == 'Published' and entity['phi'] == 'no':
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
    
    start = time.process_time()
    main = Main(index_name)
    main.main()
    end = time.process_time()
    print(end - start)