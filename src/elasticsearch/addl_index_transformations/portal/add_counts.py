from collections import Counter


def add_counts(doc):
    '''
    >>> from pprint import pprint
    >>> doc = {
    ...    'ancestors': [
    ...        {'entity_type': 'Donor'},
    ...        {'entity_type': 'Sample'},
    ...        {'entity_type': 'Sample'},
    ...    ],
    ...    'descendants': [
    ...        {'entity_type': 'Sample'},
    ...        {'entity_type': 'Sample'},
    ...        {'entity_type': 'Dataset'},
    ...    ]
    ... }
    >>> add_counts(doc)
    >>> pprint(doc['ancestor_counts'])
    {'entity_type': {'Donor': 1, 'Sample': 2}}
    >>> pprint(doc['descendant_counts'])
    {'entity_type': {'Dataset': 1, 'Sample': 2}}

    '''
    doc['ancestor_counts'] = {
        'entity_type': dict(Counter([entity['entity_type'] for entity in doc['ancestors'] if 'entity_type' in entity]))
    }
    doc['descendant_counts'] = {
        'entity_type': dict(Counter([entity['entity_type'] for entity in doc['descendants'] if 'entity_type' in entity]))
    }
