from pathlib import Path

import jsonschema
from yaml import safe_load as load_yaml


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
    clean_doc = _clean(doc)
    _validate(clean_doc)  # Caller will log errors.
    translated_doc = _translate(clean_doc)
    return translated_doc


_schema_dir = Path(__file__).parent / 'search-schema' / 'data' / 'schemas'


def _clean(doc):
    schema = _get_schema(doc)
    allowed_props = schema['properties'].keys()
    cleaned = {
        k: v for k, v in doc.items()
        if k in allowed_props
    }
    return cleaned


_schemas = {
    entity_type:
        load_yaml((
            _schema_dir / f'{entity_type}.schema.yaml'
        ).read_text())
    for entity_type in ['dataset', 'donor', 'sample']
}


def _get_schema(doc):
    entity_type = doc['entity_type'].lower()
    return _schemas[entity_type]


def _validate(doc):
    jsonschema.validate(doc, _get_schema(doc))


def _translate(doc):
    return doc
