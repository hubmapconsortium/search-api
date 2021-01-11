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
            headers = {'Content-Type': 'application/json'}
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_doc/{uuid}", headers=headers, data=doc)
            if rspn.ok:
                logger.info(f"""Added doc of uuid: {uuid} to index: {index_name}""")
            else:
                logger.error(f"""Failed to write {uuid} to elasticsearch, index: {index_name}""")
                logger.error(f"""Error Message: {rspn.text}""")
        except Exception:
            msg = "Exception encountered during executing ESWriter.write_document()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def delete_document(self, index_name, uuid):
        try:
            headers = {'Content-Type': 'application/json'}
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_delete_by_query?q=uuid:{uuid}", headers=headers)
            if rspn.ok:
                logger.info(f"""Deleted doc of uuid: {uuid} from index: {index_name}""")
            else:
                logger.error(f"""Failed to delete doc of uuid: {uuid} from index: {index_name}""")
                logger.error(f"""Error Message: {rspn.text}""")
        except Exception:
            msg = "Exception encountered during executing ESWriter.delete_document()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def write_or_update_document(self, index_name='index', type_='_doc', doc='', uuid=''):
        try:
            headers = {'Content-Type': 'application/json'}

            #logger.debug(f"Document: {doc}")

            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}/{type_}/{uuid}", headers=headers, data=doc)
            if rspn.status_code in [200, 201, 202]:
                logger.info(f"""Added doc of uuid: {uuid} to index: {index_name}""")
            else:
                logger.error(f"""Failed to write doc of uuid: {uuid} to index: {index_name}""")
                logger.error(f"""Error Message: {rspn.text}""")
        except Exception:
            msg = "Exception encountered during executing ESWriter.write_or_update_document()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def delete_index(self, index_name):
        try:
            rspn = requests.delete(f"{self.elasticsearch_url}/{index_name}")

            if rspn.ok:
                logger.info(f"""Deleted index: {index_name}""")
            else:
                logger.error(f"""Failed to delete index: {index_name} in elasticsearch.""")
                logger.error(f"""Error Message: {rspn.text}""")
        except Exception:
            msg = "Exception encountered during executing ESWriter.delete_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def create_index(self, index_name):
        try:
            headers = {'Content-Type': 'application/json'}

            index_info_dict = {
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

            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}", headers=headers, data=json.dumps(index_info_dict))
            if rspn.ok:
                logger.info(f"""Created index: {index_name}""")
            else:
                logger.error(f"""Failed to create index: {index_name} in elasticsearch.""")
                logger.error(f"""Error Message: {rspn.text}""")
        except Exception:
            msg = "Exception encountered during executing ESWriter.create_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
