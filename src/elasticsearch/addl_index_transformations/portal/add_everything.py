# Note: The goal is to support free-text search across all fields.
# This is a stop-gap until mappings can be specified, and copy_to used:
# https://github.com/hubmapconsortium/search-api/issues/63


def add_everything(doc):
    '''
    >>> doc = {
    ...   'a': {
    ...     'b': {
    ...       'c': ['', 'deep', 'deep']
    ...     }
    ...   }
    ... }
    >>> add_everything(doc)
    >>> doc
    {'a': {'b': {'c': ['', 'deep', 'deep']}}, 'everything': ['deep']}

    '''
    everything = set(_get_nested_values(doc))
    # Sort for stability in tests;
    # Could be removed if it's a performance hit.
    doc['everything'] = sorted(everything)


def _get_nested_values(input):
    '''
    >>> doc = {
    ...   'a': {
    ...     'b0': {
    ...       'c': ['', 'deep', 'deep!']
    ...     },
    ...     'b1': 'deep'
    ...   },
    ...   'xyz': 'shallow',
    ...   'number': 123
    ... }
    >>> list(_get_nested_values(doc))
    ['deep', 'deep!', 'deep', 'shallow', '123']

    '''
    if isinstance(input, dict):
        for value in input.values():
            yield from _get_nested_values(value)
    elif isinstance(input, list):
        for value in input:
            yield from _get_nested_values(value)
    elif input:
        yield str(input)
