def sort_files(doc):
    '''
    >>> from pprint import pprint
    >>> doc = {
    ...    'files': [
    ...         {'rel_path': './B.txt', 'capitals': 'sorted correctly'},
    ...         {'rel_path': './a.txt'},
    ...         {'rel_path': './c.txt'}
    ...     ]
    ... }
    >>> sort_files(doc)
    >>> pprint(doc)
    {'files': [{'rel_path': './a.txt'},
               {'capitals': 'sorted correctly', 'rel_path': './B.txt'},
               {'rel_path': './c.txt'}]}

    '''
    if 'files' in doc:
        doc['files'].sort(key=lambda file: file['rel_path'].lower())
