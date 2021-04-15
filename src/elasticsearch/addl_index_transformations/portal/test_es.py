#!/usr/bin/env python
from pathlib import Path

import requests
from yaml import safe_load as load_yaml

base_url = 'http://127.0.0.1:9200'
index = 'test_index'


def setup_function(function):
    base_response = requests.get(base_url).json()
    assert base_response['version']['number'].startswith('7.')

    delete_response = requests.delete(f'{base_url}/{index}').json()
    assert 'error' in delete_response or delete_response['acknowledged']

    get_deleted_index_response = requests.get(f'{base_url}/{index}').json()
    assert get_deleted_index_response['error']['type'] == 'index_not_found_exception'

    config = load_yaml((Path(__file__).parent / 'config.yaml').read_text())
    put_index_response = requests.put(f'{base_url}/{index}', headers={'Content-Type': 'application/json'}, json=config).json()
    assert put_index_response['acknowledged']

    # get_index_response = requests.get(f'{base_url}/{index}').json()
    # assert 'dynamic_templates' in get_index_response[index]['mappings']


def test_tokenization_and_search():
    doc = {
        'description': 'Lorem ipsum dolor sit amet',
    }
    # NOTE: Without '?refresh', the index is not guaranteed to be
    # up-to-date when the response returns. Should not be unused
    # in production, but necessary for a synchronous test like this.
    put_doc_response = requests.put(f'{base_url}/{index}/_doc/1?refresh', json=doc).json()
    assert '_index' in put_doc_response

    # Confirm that it is indexed:

    get_doc_response = requests.get(f'{base_url}/{index}/_doc/1').json()
    assert 'description' in get_doc_response['_source']

    query = {'query': {'match': {'all_text': {
        # NOTE: This is just one word from the string,
        # so it's testing whether tokenization works.
        'query': 'Lorem'
    }}}}
    headers = {'Content-Type': 'application/json'}
    get_search_response = requests.request(
        'GET',
        url=f'{base_url}/{index}/_search',
        headers=headers,
        json=query
    ).json()
    assert len(get_search_response['hits']['hits']) == 1
    assert 'all_text' not in get_search_response['hits']['hits'][0]['_source']


def test_sort_by_keyword():
    docs = [
        {'animal': 'zebra'},
        {'animal': 'ant'},
        {'animal': 'bear'},
        {'animal': 'cat'},
    ]
    for i, doc in enumerate(docs):
        requests.put(f'{base_url}/{index}/_doc/{i}?refresh', json=doc).json()

    query = {
        # TODO: Add tests to make sure all parts of the query work as intended.
        # 'post_filter': {},
        # 'aggs': {}
        'sort': [{'animal.keyword': 'asc'}],
        # 'highlight': {},
        '_source': ['animal']
    }

    search_response = requests.post(f'{base_url}/{index}/_search', json=query).json()
    assert [
        hit['_source']['animal']
        for hit in search_response['hits']['hits']
    ] == ['ant', 'bear', 'cat', 'zebra']


def test_sort_by_relevance():
    docs = [
        {'item': 'duck duck goose'},
        {'item': 'duck duck grey duck'},
        {'item': 'donald duck'},
        {'item': 'your goose is cooked'},
    ]
    for i, doc in enumerate(docs):
        requests.put(f'{base_url}/{index}/_doc/{i}?refresh', json=doc).json()

    def search(term):
        query = {'query': {'match': {'item': term}}}
        search_response = requests.post(f'{base_url}/{index}/_search', json=query).json()
        return [
            hit['_source']['item']
            for hit in search_response['hits']['hits']
        ]

    # With norms, results are returned in order of relevance:
    assert search('duck') == ['duck duck grey duck', 'duck duck goose', 'donald duck']
    assert search('goose') == ['duck duck goose', 'your goose is cooked']
