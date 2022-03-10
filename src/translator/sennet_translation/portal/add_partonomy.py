import argparse
from collections import defaultdict
import requests
from pathlib import Path
from json import loads, dumps
from yaml import safe_load


_two_letter_to_iri = {
    two_letter: organ.get('iri')
    for two_letter, organ
    in safe_load(
        (Path(__file__).parent.parent
         / 'search-schema/data/definitions/enums/organ_types.yaml').read_text()
    ).items()
}


def _get_organ_iri(doc):
    two_letter_code = doc.get('origin_sample', {}).get('organ')
    return _two_letter_to_iri.get(two_letter_code)


def add_partonomy(doc):
    '''
    >>> rui_location = {
    ...     'ccf_annotations': [
    ...         'http://purl.obolibrary.org/obo/UBERON_0001157',
    ...         'http://example.com/some-other-random-id'
    ...     ]
    ... }
    >>> from json import dumps
    >>> doc = {
    ...     'rui_location': dumps(rui_location)
    ... }
    >>> add_partonomy(doc)
    >>> del doc['rui_location']
    >>> doc
    {'anatomy_0': ['body'], 'anatomy_1': ['large intestine'], 'anatomy_2': ['transverse colon']}

    >>> doc = {
    ...     'origin_sample': {'organ': 'RK'},
    ... }
    >>> add_partonomy(doc)
    >>> del doc['origin_sample']
    >>> doc
    {'anatomy_0': ['body'], 'anatomy_1': ['kidney'], 'anatomy_2': ['right kidney']}

    If there are both:

    >>> doc = {
    ...     'origin_sample': {'organ': 'RK'},
    ...     'rui_location': dumps(rui_location)
    ... }
    >>> add_partonomy(doc)
    >>> del doc['origin_sample']
    >>> del doc['rui_location']
    >>> doc
    {'anatomy_0': ['body'], 'anatomy_1': ['kidney', 'large intestine'], 'anatomy_2': ['right kidney', 'transverse colon']}

    New organ code: No error.

    >>> doc = {
    ...     'origin_sample': {'organ': 'ZZ'},
    ... }
    >>> add_partonomy(doc)
    >>> del doc['origin_sample']
    >>> doc
    {}

    What if everything is missing?

    >>> doc = {}
    >>> add_partonomy(doc)
    >>> doc
    {}

    '''
    annotations = []

    organ_iri = _get_organ_iri(doc)
    if organ_iri:
        annotations.append(organ_iri)

    if 'rui_location' in doc:
        if isinstance(doc['rui_location'], str):
            rui_location = loads(doc['rui_location'])
        else:
            rui_location = doc['rui_location']
        annotations += rui_location.get('ccf_annotations', [])

    partonomy_sets_doc = defaultdict(set)
    for uri in annotations:
        ancestor_list = _get_ancestors_of(uri, index)
        ancestor_facets = _make_dict_from_ancestors(ancestor_list)
        for facet, term_set in ancestor_facets.items():
            partonomy_sets_doc[facet] |= term_set

    partonomy_lists_doc = {k: sorted(v) for k, v in partonomy_sets_doc.items()}
    doc.update(partonomy_lists_doc)


def _get_ancestors_of(node_id, index):
    if node_id not in index:
        return []
    node = index[node_id]
    ancestors = _get_ancestors_of(node['parent_id'], index) if node['parent_id'] else []
    ancestors.append(node['value'])
    return ancestors


def _make_dict_from_ancestors(ancestors):
    numbered = enumerate(ancestors)
    return {
        f'anatomy_{i}': set([term])
        for i, term in numbered
    }


def _build_tree_index():
    '''
    Returns a tuple:
        - A tree, where each node has value, parent_id, and children.
        - A dict indexing into every node of the tree by id.

    >>> tree, index = _build_tree_index()
    >>> from pprint import pprint
    >>> pprint(tree, depth=1)
    {'children': [...], 'parent_id': None, 'value': 'body'}
    >>> pprint(sorted([child['value'] for child in tree['children']]))
    ['blood',
     'bone marrow',
     'brain',
     'heart',
     'kidney',
     'kidney vasculature',
     'large intestine',
     'lymph node',
     'pelvis',
     'respiratory system',
     'retromandibular vein',
     'skin',
     'spleen',
     'spleen',
     'spleen',
     'telencephalic ventricle',
     'thymus']
    >>> pprint(index['http://purl.obolibrary.org/obo/UBERON_0000029'], depth=1)
    {'children': [...],
     'parent_id': 'http://purl.obolibrary.org/obo/UBERON_0013702',
     'value': 'lymph node'}
    '''
    parent_path = Path(__file__).parent
    version = (parent_path / 'partonomy-version.txt').read_text().strip()
    partonomy_path = parent_path / f'cache/partonomy-{version}.jsonld'
    if not partonomy_path.exists():
        partonomy_url = f'https://cdn.jsdelivr.net/gh/hubmapconsortium/hubmap-ontology@{version}/ccf-partonomy.jsonld'
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

    return simplified[root_id], simplified


tree, index = _build_tree_index()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Given RUI-JSON, return the anatomy facets that will be generated.'
    )

    parser.add_argument('rui_json', help='RUI-JSON as string.')
    args = parser.parse_args()
    doc = {'rui_location': args.rui_json}
    add_partonomy(doc)
    del doc['rui_location']
    print(dumps(doc, sort_keys=True, indent=2))
