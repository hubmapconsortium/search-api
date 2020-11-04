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

    >>> doc = {
    ...    'ancestors': [],
    ...    'descendants': [{
    ...        'entity_type': 'Sample',
    ...        'mapped_specimen_type': 'Fresh Frozen Tissue Section',
    ...        'mapped_organ': 'Lymph Node',
    ...        'mapped_data_types': ['Autofluorescence Microscopy']
    ...    }]
    ... }
    >>> add_counts(doc)
    >>> pprint(doc['descendant_counts'])
    {'entity_type': {'Sample': 1},
     'mapped_data_types': {'Autofluorescence Microscopy': 1},
     'mapped_organ': {'Lymph Node': 1},
     'mapped_specimen_type': {'Fresh Frozen Tissue Section': 1}}

    '''
    # Collections do not have ancestors or descendants.
    if 'ancestors' in doc:
        doc['ancestor_counts'] = {
            'entity_type': _count_field(doc['ancestors'], 'entity_type')
        }
    if 'descendants' in doc:
        doc['descendant_counts'] = {k: v for k, v in {
            'entity_type': _count_field(doc['descendants'], 'entity_type'),
            'mapped_specimen_type': _count_field(doc['descendants'], 'mapped_specimen_type'),
            'mapped_organ': _count_field(doc['descendants'], 'mapped_organ'),
            'mapped_data_types': _count_array_field(doc['descendants'], 'mapped_data_types'),
        }.items() if v}


def _count_field(doc_list, field):
    return dict(Counter([
        entity[field] for entity in doc_list
        if field in entity
    ]))


def _count_array_field(doc_list, field):
    return dict(Counter(_flatten([
        entity[field] for entity in doc_list
        if field in entity
    ])))


def _flatten(a_list):
    return sum(a_list, [])
