def add_publication_status(doc):
    '''
    >>> from pprint import pprint

    >>> doc = {
    ...    'descendant_counts': {'entity_type': {'Donor': 1, 'Sample': 2}}
    ... }
    >>> add_publication_status(doc)
    >>> pprint(doc['has_publication'])
    False

    >>> doc = {
    ...    'descendant_counts': {}
    ... }
    >>> add_publication_status(doc)
    >>> pprint(doc['has_publication'])
    False

    >>> doc = {
    ...    'descendant_counts': {'entity_type': {'Publication': 1, 'Sample': 2}}
    ... }
    >>> add_publication_status(doc)
    >>> pprint(doc['has_publication'])
    True

    >>> doc = {
    ...    'descendant_counts': {'entity_type': {'Publication': 14}}
    ... }
    >>> add_publication_status(doc)
    >>> pprint(doc['has_publication'])
    True

    '''

    doc['has_publication'] = False

    if 'descendant_counts' in doc:
        publication_count = doc['descendant_counts'].get('entity_type', {}).get('Publication', 0)
        doc['has_publication'] = publication_count > 0
        return
