#!/usr/bin/env python

import requests
from json import loads

base_url = 'http://127.0.0.1:9200'

base_response = requests.get('http://127.0.0.1:9200').json()
assert 'cluster_name' in base_response

# TODO: Confirm that indexing works as expected.
