def reset_entity_type(doc):
    '''
    In the "Dataset" search results, we don't want to show "Datasets"
    which are only there to provide visualization.
    As vis-lifting is implemented in portal-ui, we will match
    the corresponding entities here, and reset the entity_type.

    >>> doc = {
    ...     'data_types': ['image_pyramid'],
    ...     'entity_type': 'Dataset'
    ... }
    >>> reset_entity_type(doc)
    >>> doc['entity_type']
    'Support'

    '''
    if 'data_types' not in doc:
        return
    if 'image_pyramid' in doc['data_types']:
        doc['entity_type'] = 'Support'
