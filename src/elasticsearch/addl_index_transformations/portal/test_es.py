#!/usr/bin/env python
from pathlib import Path
import logging

import requests
from yaml import safe_load as load_yaml


LOGGER = logging.getLogger(__name__)

base_url = 'http://127.0.0.1:9200'
index = 'test_index'


def setup_module(module):
    base_response = requests.get(base_url).json()
    LOGGER.info(f'base:\t{base_response}')
    assert base_response['version']['number'].startswith('7.')

    delete_response = requests.delete(f'{base_url}/{index}').json()
    LOGGER.info(f'delete:\t{delete_response}')
    assert 'error' in delete_response or delete_response['acknowledged']

    get_deleted_index_response = requests.get(f'{base_url}/{index}').json()
    LOGGER.info(f'get index:\t{get_deleted_index_response}')
    assert get_deleted_index_response['error']['type'] == 'index_not_found_exception'

    config = load_yaml((Path(__file__).parent / 'config.yaml').read_text())
    put_index_response = requests.put(f'{base_url}/{index}', headers={'Content-Type': 'application/json'}, json=config).json()
    LOGGER.info(f'put index:\t{put_index_response}')
    assert put_index_response['acknowledged']

    get_index_response = requests.get(f'{base_url}/{index}').json()
    LOGGER.info(f'get index:\t{get_index_response}')
    assert 'dynamic_templates' in get_index_response[index]['mappings']


def test_elasticsearch():
    # Add a document:

    doc = {
        "description": "Lorem ipsum dolor sit amet",
    }
    # NOTE: Without "?refresh", the index is not guaranteed to be
    # up-to-date when the response returns. Should not be unused
    # in production, but necessary for a synchronous test like this.
    put_doc_response = requests.put(f'{base_url}/{index}/_doc/1?refresh', json=doc).json()
    LOGGER.info(f'put doc:\t{put_doc_response}')
    assert '_index' in put_doc_response

    # Confirm that it is indexed:

    get_doc_response = requests.get(f'{base_url}/{index}/_doc/1').json()
    LOGGER.info(f'get doc:\t{get_doc_response}')
    assert 'description' in get_doc_response['_source']

    query = {'query': {'match': {'all_text': {
        "query": "Lorem"
    }}}}
    headers = {'Content-Type': 'application/json'}
    get_search_response = requests.request(
        'GET',
        url=f'{base_url}/{index}/_search',
        headers=headers,
        json=query
    ).json()
    LOGGER.info(f'get query:\t{get_search_response}')
    assert len(get_search_response['hits']['hits']) == 1
    assert 'all_text' not in get_search_response['hits']['hits'][0]['_source']
