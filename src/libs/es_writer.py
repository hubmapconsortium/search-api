import requests
import json
import os
import logging

class ESWriter:
    def __init__(self, elasticsearch_url):
        self.elasticsearch_url = elasticsearch_url

    def write_document(self, index_name, doc, uuid):
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_doc/{uuid}",
                            headers={'Content-Type': 'application/json'},
                            data=doc)
            if rspn.ok:
                logging.info("write doc done")
            else:
                logging.error(f"""error happened when writing {uuid} to elasticsearch\n
                        Error Message: {rspn.text}""")
        except Exception as e:
            logging.error(str(e))

        # rspn = requests.get(f"{self.elasticsearch_url}/{index_name}/_search?pretty")

    def delete_document(self, index_name, uuid):
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_delete_by_query?q=uuid:{uuid}",
                            headers={'Content-Type': 'application/json'})
            if rspn.ok:
                logging.info(f"doc: {uuid} deleted")
            else:
                logging.error(rspn.text)
        except Exception as e:
            logging.error(str(e))

    def write_or_update_document(self, index_name, doc, uuid):
        try:
            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}/_doc/{uuid}",
                            headers={'Content-Type': 'application/json'},
                            data=doc)
            if rspn.ok:
                logging.info("write doc done")
            else:
                logging.error(f"""error happened when writing {uuid} to elasticsearch\n
                        Error Message: {rspn.text}""")
                logging.error(f"Document: {doc}")
        except Exception as e:
            logging.error(str(e))

    def remove_index(self, index_name):
        rspn = requests.delete(f"{self.elasticsearch_url}/{index_name}")
    
    def create_index(self, index_name):
        try:
            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}", 
                                headers={'Content-Type': 'application/json'},
                                data=json.dumps({"settings": {"index" : {
                                                            "mapping.total_fields.limit": 5000,
                                                            "query.default_field": 2048,
                                                            "number_of_shards": 1,
                                                            "number_of_replicas": 1}}}))
            if rspn.ok:
                logging.info(f"index {index_name} created")
            else:
                logging.error(f"""error happened when creating {index_name} on elasticsearch\n
                        Error Message: {rspn.text}""")
        except Exception as e:
            logging.error(str(e))
# if __name__ == '__main__':
#     db_reader = DBReader({'NEO4J_SERVER':'bolt://18.205.215.12:7687', 'NEO4J_USERNAME': 'neo4j', 'NEO4J_PASSWORD': 'td8@-F7yC8cjrJ?3'})
#     node = db_reader.get_donor('TEST0010')
#     es_writer = ESWriter({'ELASTICSEARCH_DOMAIN_ENDPOINT': 'https://search-hubmap-entity-es-dev-zhdpuhhf2vjpvqfq7zmn2gdgqq.us-east-1.es.amazonaws.com'})
#     es_writer.write_document(json.dumps(node))