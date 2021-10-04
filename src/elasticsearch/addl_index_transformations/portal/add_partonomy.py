import requests
from pathlib import Path
from json import loads
from yaml import safe_load


_two_letter_to_iri = {
    two_letter: organ.get('iri')
    for two_letter, organ
    in safe_load(Path(
        __file__.parent.parent.parent.parent /
        'search-schema/data/definitions/enums/organ_types.yaml'
    )).items()
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
    ...     'origin_sample': {
    ...         'organ': 'LI'
    ...     },
    ...     'rui_location': dumps(rui_location)
    ... }
    >>> add_partonomy(doc)
    >>> del doc['rui_location']
    >>> del doc['origin_sample']
    >>> doc
    {'anatomy_0': 'body', 'anatomy_1': 'large intestine', 'anatomy_2': 'transverse colon'}

    '''
    annotations = []

    organ_iri = _get_organ_iri(doc)
    if organ_iri:
        annotations.append(organ_iri)

    if 'rui_location' in doc:
        rui_location = loads(doc['rui_location'])

        annotations += rui_location.get('ccf_annotations', [])

    for uri in annotations:
        ancestor_list = _get_ancestors_of(uri, index)
        dict_to_merge = _make_dict_from_ancestors(ancestor_list)
        doc.update(dict_to_merge)


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
        f'anatomy_{i}': term
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
