def reset_entity_type(doc):
    '''
    In the "Dataset" search results, we don't want to show "Datasets"
    which are only there to provide visualization.
    As vis-lifting is implemented in portal-ui, we will match
    the corresponding entities here, and reset the entity_type.

    >>> doc = {
    ...     'vitessce-hints': ['is_support'],
    ...     'entity_type': 'Dataset'
    ... }
    >>> reset_entity_type(doc)
    >>> doc['entity_type']
    'Support'

    >>> doc = {
    ...     'vitessce-hints': [],
    ...     'entity_type': 'Dataset'
    ... }
    >>> reset_entity_type(doc)
    >>> doc['entity_type']
    'Dataset'

    '''
    if 'vitessce-hints' not in doc:
        return
    if 'is_support' in doc['vitessce-hints']:
        doc['entity_type'] = 'Support'

