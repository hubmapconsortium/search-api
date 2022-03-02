import json
import logging
import os
from pathlib import Path

from TranslatorInterface import IndexerInterface
from libs.es_writer import ESWriter

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class HuBMAPIndexer(IndexerInterface):
    def reindex_all(self):
        pass

    def reindex(self, entity_id):
        pass

    def create_doc(self, entity_id):
        pass

    def update_doc(self, entity_id, entity_info):
        pass

    def __init__(self, indices, app_client_id, app_client_secret, token):
        try:
            self.indices: dict = {}
            # Do not include the indexes that are self managed...
            for key, value in indices['indices'].items():
                if 'reindex_enabled' in value and value['reindex_enabled'] is True:
                    self.indices[key] = value
            self.DEFAULT_INDEX_WITHOUT_PREFIX: str = indices['default_index']
            self.INDICES: dict = {'default_index': self.DEFAULT_INDEX_WITHOUT_PREFIX, 'indices': self.indices}
            logger.debug("@@@@@@@@@@@@@@@@@@@@ INDICES")
            logger.debug(self.INDICES)
        except Exception:
            raise ValueError("Invalid indices config")

        self.elasticsearch_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['elasticsearch']['url'].strip('/')

        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token

        auth_helper = self.init_auth_helper()
        self.request_headers = self.create_request_headers_for_auth(token)

        self.eswriter = ESWriter(self.elasticsearch_url)
        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')

        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent.parent / 'VERSION').read_text()).strip()

        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'neo4j-to-es-attributes.json'),
                  'r') as json_file:
            self.attr_map = json.load(json_file)

        # Preload all the transformers
        self.init_transformers()
