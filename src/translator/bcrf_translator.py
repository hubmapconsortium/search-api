import logging
from pathlib import Path

from indexer import Indexer
from translator.translator_interface import TranslatorInterface


logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class Translator(TranslatorInterface):
    def __init__(self, indices, app_client_id, app_client_secret, token):
        try:
            self.indices: dict = {}
            # Do not include the indexes that are self managed...
            for key, value in indices['indices'].items():
                if 'reindex_enabled' in value and value['reindex_enabled'] is True:
                    self.indices[key] = value
            self.DEFAULT_INDEX_WITHOUT_PREFIX: str = indices['default_index']
            self.INDICES: dict = {'default_index': self.DEFAULT_INDEX_WITHOUT_PREFIX, 'indices': self.indices}
            self.DEFAULT_ENTITY_API_URL = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX][
                'document_source_endpoint'].strip(
                '/')

            self.indexer = Indexer(self.indices, self.DEFAULT_INDEX_WITHOUT_PREFIX)

            logger.debug("@@@@@@@@@@@@@@@@@@@@ INDICES")
            logger.debug(self.INDICES)
        except Exception:
            raise ValueError("Invalid indices config")

        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token

        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')

        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent.parent / 'VERSION').read_text()).strip()

    def translate_all(self):
        pass

    def translate(self, entity_id):
        pass

    def update(self, entity_id, document):
        for index in self.indices.keys():
            public_index = self.INDICES['indices'][index]['public']
            private_index = self.INDICES['indices'][index]['private']

            if self.is_public(document):
                self.indexer.index(entity_id, document, public_index, True)

            self.indexer.index(entity_id, document, private_index, True)

    def add(self, entity_id, document):
        for index in self.indices.keys():
            public_index = self.INDICES['indices'][index]['public']
            private_index = self.INDICES['indices'][index]['private']

            if self.is_public(document):
                self.indexer.index(entity_id, document, public_index, False)

            self.indexer.index(entity_id, document, private_index, False)

    def is_public(self, document):
        return False
