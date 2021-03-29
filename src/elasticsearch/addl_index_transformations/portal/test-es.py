#!/usr/bin/env python

from json import loads
from pathlib import Path

import requests
from yaml import safe_load as load_yaml


base_url = 'http://127.0.0.1:9200'

base_response = requests.get(base_url).json()
assert 'cluster_name' in base_response

# Set up clean index:

index = 'test_index'
delete_response = requests.delete(f'{base_url}/{index}').json()
print(delete_response)
assert 'error' in delete_response or delete_response['acknowledged']

config = load_yaml((Path(__file__).parent / 'config.yaml').read_text())
put_index_response = requests.put(f'{base_url}/{index}').json()
print(put_index_response)
assert put_index_response['acknowledged']

# Add a document:

doc = {'new_unexpected_field': 'XYZ'}
put_doc_response = requests.put(f'{base_url}/{index}/_doc/1', json=doc).json()
print(put_doc_response)
assert '_index' in put_doc_response

# Confirm that it is indexed:

get_doc_response = requests.get(f'{base_url}/{index}/_doc/1').json()
print(get_doc_response)
assert get_doc_response['_source']['new_unexpected_field'] == 'XYZ'

query = {'query': {'match': {'all_text': {
  'query': 'XYZ',
  'operator': 'and'
}}}}
headers = {'Content-Type': 'application/json'}
get_search_response = requests.get(f'{base_url}/{index}/_search', headers=headers, json=query).json()
print(get_search_response)
assert get_search_response == 'foo'

# TODO: Confirm that indexing works as expected.

print('No errors!')
