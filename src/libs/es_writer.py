import requests
import json
import os

class ESWriter:
    def __init__(self, elasticsearch_url):
        self.elasticsearch_url = elasticsearch_url

    def wrtire_document(self, index_name, doc):
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_doc",
                            headers={'Content-Type': 'application/json'},
                            data=doc)
            if rspn.ok:
                print("OK")
            else:
                print(rspn.text)
        except Exception as e:
            print(str(e))

        # rspn = requests.get(f"{self.elasticsearch_url}/{index_name}/_search?pretty")

    def remove_index(self, index_name):
        rspn = requests.delete(f"{self.elasticsearch_url}/{index_name}")

# if __name__ == '__main__':
#     db_reader = DBReader({'NEO4J_SERVER':'bolt://18.205.215.12:7687', 'NEO4J_USERNAME': 'neo4j', 'NEO4J_PASSWORD': 'td8@-F7yC8cjrJ?3'})
#     node = db_reader.get_donor('TEST0010')
#     es_writer = ESWriter({'ELASTICSEARCH_DOMAIN_ENDPOINT': 'https://search-hubmap-entity-es-dev-zhdpuhhf2vjpvqfq7zmn2gdgqq.us-east-1.es.amazonaws.com'})
#     es_writer.wrtire_document(json.dumps(node))