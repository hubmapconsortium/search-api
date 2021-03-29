#!/usr/bin/env python

from json import loads
from pathlib import Path

import requests
from yaml import safe_load as load_yaml


base_url = 'http://127.0.0.1:9200'

base_response = requests.get(base_url).json()
assert 'cluster_name' in base_response

index = 'test_index'
delete_response = requests.delete(f'{base_url}/{index}').json()
print(delete_response)
assert 'error' in delete_response or delete_response['acknowledged']

config = load_yaml((Path(__file__).parent / 'config.yaml').read_text())
put_index_response = requests.put(f'{base_url}/{index}').json()
print(put_index_response)
assert put_index_response['acknowledged']

put_doc_response = requests.put(f'{base_url}/{index}/_doc/1', json={'name': 'XYZ'}).json()
print(put_doc_response)
assert '_index' in put_doc_response

get_doc_response = requests.get(f'{base_url}/{index}/_doc/1').json()
print(get_doc_response)
assert get_doc_response['_source']['name'] == 'XYZ'

# TODO: Confirm that indexing works as expected.

print('No errors!')
