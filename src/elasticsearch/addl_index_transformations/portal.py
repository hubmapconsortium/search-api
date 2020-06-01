from pathlib import Path
from copy import deepcopy

import jsonschema
from yaml import safe_load as load_yaml

from portal_translate import translate


def transform(doc, batch_id='unspecified'):
    '''
    >>> transform({})
    Traceback (most recent call last):
    ...
    KeyError: 'entity_type'

    >>> from pprint import pprint
    >>> pprint(transform({
    ...    'entity_type': 'Donor',
    ...    'create_timestamp': 0,
    ...    'created_by_user_displayname': 'xxx',
    ...    'created_by_user_email': 'xxx',
    ...    'group_name': 'xxx',
    ...    'group_uuid': 'xxx',
    ...    'last_modified_timestamp': 'xxx',
    ...    'uuid': 'xxx',
    ...    'access_group': 'xxx',
    ...    'ancestor_ids': 'xxx',
    ...    'ancestors': 'xxx',
    ...    'descendant_ids': 'xxx',
    ...    'descendants': 'xxx',
    ...    'THE_SPANISH_INQUISITION': 'No one expects'
    ... }))
    {'access_group': 'xxx',
     'ancestor_ids': 'xxx',
     'ancestors': 'xxx',
     'create_timestamp': 0,
     'created_by_user_displayname': 'xxx',
     'created_by_user_email': 'xxx',
     'descendant_ids': 'xxx',
     'descendants': 'xxx',
     'entity_type': 'Donor',
     'group_name': 'xxx',
     'group_uuid': 'xxx',
     'last_modified_timestamp': 'xxx',
     'uuid': 'xxx'}

    '''
    doc_copy = deepcopy(doc)
    # We will modify in place below,
    # so make a deep copy so we don't surprise the caller.
    _clean(doc_copy)
    _validate(doc_copy)  # Caller will log errors.
    translate(doc_copy)
    return doc_copy


_data_dir = Path(__file__).parent / 'search-schema' / 'data'


def _clean(doc):
    schema = _get_schema(doc)
    allowed_props = schema['properties'].keys()
    keys = list(doc.keys())
    for key in keys:
        if key not in allowed_props:
            del doc[key]


_schemas = {
    entity_type:
        load_yaml((
            _data_dir / 'schemas' / f'{entity_type}.schema.yaml'
        ).read_text())
    for entity_type in ['dataset', 'donor', 'sample']
}


def _get_schema(doc):
    entity_type = doc['entity_type'].lower()
    return _schemas[entity_type]


def _validate(doc):
    jsonschema.validate(doc, _get_schema(doc))
