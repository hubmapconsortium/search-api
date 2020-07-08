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
        'entity_type': _count_field(doc['ancestors'], 'entity_type')
    }
    doc['descendant_counts'] = {
        'entity_type': _count_field(doc['descendants'], 'entity_type')
    }


def _count_field(doc_list, field):
    return dict(Counter([entity[field] for entity in doc_list if field in entity]))
