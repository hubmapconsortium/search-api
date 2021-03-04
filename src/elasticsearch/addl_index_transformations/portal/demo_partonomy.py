import requests
from pathlib import Path
from json import loads


def build_tree():
    partonomy_path = Path(__name__).parent / 'cache/partonomy.jsonld'
    if not partonomy_path.exists():
        partonomy_url = 'https://cdn.jsdelivr.net/gh/hubmapconsortium/hubmap-ontology@1.0.0/ccf-partonomy.jsonld'
        partonomy_path.write_text(requests.get(partonomy_url).text)

    partonomy_ld = loads(partonomy_path.read_text())
    simplified = {
        node['@id']: 
        {
            'value': node['http://www.w3.org/2000/01/rdf-schema#label'][0]['@value'],
            'parent_id': node['parent'][0]['@id'] if node['parent'] else None,
            'children': []
        } for node in partonomy_ld
    }

    for node_id, node in simplified.items():
        parent_id = node['parent_id']
        if parent_id:
            simplified[parent_id]['children'].append(node)
        else:
            root_id = node_id
    
    return simplified[root_id]

from pprint import pprint
pprint(build_tree())
