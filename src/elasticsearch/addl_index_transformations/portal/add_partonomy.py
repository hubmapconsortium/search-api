import requests
from pathlib import Path
from json import loads


def add_partonomy(doc):
    '''
    >>> rui_location = {
    ...     'ccf_annotations': ['http://purl.obolibrary.org/obo/UBERON_0001157']
    ... }
    >>> from json import dumps
    >>> doc = {
    ...     'rui_location': dumps(rui_location)
    ... }
    >>> add_partonomy(doc)
    >>> del doc['rui_location']
    >>> doc
    {'anatomy_0': 'body', 'anatomy_1': 'abdominal cavity', 'anatomy_2': 'colon', 'anatomy_3': 'transverse colon'}

    '''
    if 'rui_location' not in doc:
        return
    rui_location = loads(doc['rui_location'])

    if 'ccf_annotations' not in rui_location:
        return
    ccf_annotations = rui_location['ccf_annotations']

    for uri in ccf_annotations:
        ancestor_list = _get_ancestors_of(uri, index)
        dict_to_merge = _make_dict_from_ancestors(ancestor_list)
        doc.update(dict_to_merge)


def _get_ancestors_of(node_id, index):
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
    '''
    partonomy_path = Path(__file__).parent / 'cache/partonomy.jsonld'
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

    return simplified[root_id], simplified


tree, index = _build_tree_index()
