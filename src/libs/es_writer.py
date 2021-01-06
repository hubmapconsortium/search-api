import requests
import json
import os
import logging
import sys

# Set logging fromat and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class ESWriter:
    def __init__(self, elasticsearch_url):
        self.elasticsearch_url = elasticsearch_url

    def write_document(self, index_name, doc, uuid):
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_doc/{uuid}",
                                 headers={'Content-Type': 'application/json'},
                                 data=doc)
            if rspn.ok:
                logger.debug("write doc done")
            else:
                logger.error(f"""error happened when writing {uuid} to elasticsearch, index: {index_name}\n
                        Error Message: {rspn.text}""")
        except Exception as e:
            logger.error(str(e))

        # rspn = requests.get(f"{self.elasticsearch_url}/{index_name}/_search?pretty")

    def delete_document(self, index_name, uuid):
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_delete_by_query?q=uuid:{uuid}",
                                 headers={'Content-Type': 'application/json'})
            if rspn.ok:
                logger.info(f"doc: {uuid} deleted")
            else:
                logger.error(rspn.text)
        except Exception as e:
            logger.error(str(e))

    def write_or_update_document(self, index_name='index', type_='_doc', doc='', uuid=''):
        try:
            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}/{type_}/{uuid}",
                                headers={'Content-Type': 'application/json'},
                                data=doc)
            if rspn.ok:
                logger.debug(f"write doc done. UUID: {uuid}")
                return True
            else:
                logger.error(f"""error happened when writing {uuid} to elasticsearch, index: {index_name}\n
                        Error Message: {rspn.text}""")
                logger.error(f"Document: {doc}")
                return False
        except Exception as e:
            logger.error(str(e))

    def remove_index(self, index_name):
        rspn = requests.delete(f"{self.elasticsearch_url}/{index_name}")

    def create_index(self, index_name):
        from elasticsearch.indexer import REPLICATION
        try:
            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}", 
                                headers={'Content-Type': 'application/json'},
                                data=json.dumps({"settings": {"index" : {
                                                            "mapping.total_fields.limit": 5000,
                                                            "query.default_field": 2048,
                                                            "number_of_shards": 1,
                                                            "number_of_replicas": REPLICATION}},
                                                "mappings": {
                                                    "date_detection": False
                                                }}))
            if rspn.ok:
                logger.info(f"index {index_name} created")
            else:
                logger.error(f"""error happened when creating {index_name} on elasticsearch\n
                        Error Message: {rspn.text}""")
        except Exception as e:
            logger.error(str(e))
