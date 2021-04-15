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


def index_docs(docs):
    for i, doc in enumerate(docs):
        # NOTE: Without '?refresh', the index is not guaranteed to be
        # up-to-date when the response returns. Should not be unused
        # in production, but necessary for a synchronous test like this.
        requests.put(f'{base_url}/{index}/_doc/{i}?refresh', json=doc).json()


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
    search_response = requests.post(f'{base_url}/{index}/_search', json=query).json()
    assert len(search_response['hits']['hits']) == 1
    assert 'all_text' not in search_response['hits']['hits'][0]['_source']


def test_sort_by_keyword():
    index_docs([
        {'animal': 'zebra'},
        {'animal': 'ant'},
        {'animal': 'bear'},
        {'animal': 'cat'},
    ])

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
    # I had thought that sorting by relevance would not work without norms...
    # (and since the portal always explicitly sorts by a field, that's fine)
    # ... but it still seems to work?
    # My guess is that it now needs to calculate relevance after the results are retrieved ...
    # but also possible that I don't have it configured correctly.
    # Since the goal is to save disk space, that's the real test we need to do.

    index_docs([
        {'item': 'duck duck goose'},
        {'item': 'duck duck grey duck'},
        {'item': 'donald duck'},
        {'item': 'your goose is cooked'},
    ])

    def search(term):
        query = {'query': {'match': {'item': term}}}
        search_response = requests.post(f'{base_url}/{index}/_search', json=query).json()
        return [
            hit['_source']['item']
            for hit in search_response['hits']['hits']
        ]

    assert search('duck') == ['duck duck grey duck', 'duck duck goose', 'donald duck']
    assert search('goose') == ['duck duck goose', 'your goose is cooked']


def test_highlight():
    index_docs([
        {'start': 'Term at the start'},
        {'end': 'or at the end: term'},
        {'repeated': 'lorem impsum TERM repeated TERM dolor sit amet'},
        {'separate': f'Term? {" " * 100} Term!'},  # Default window is 100 characters
        {'missing': 'not here'}
    ])

    query = {
        'query': {'match': {'all_text': 'term'}},
        'highlight': {'fields': {'*': {}}}
    }
    search_response = requests.post(f'{base_url}/{index}/_search', json=query).json()
    assert [
        hit['highlight']['all_text']
        for hit in search_response['hits']['hits']
    ] == [
        ['lorem impsum <em>TERM</em> repeated <em>TERM</em> dolor sit amet'],
        ['<em>Term</em>?', '<em>Term</em>!'],
        ['<em>Term</em> at the start'],
        ['or at the end: <em>term</em>']
    ]
