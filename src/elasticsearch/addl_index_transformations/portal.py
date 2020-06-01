from pathlib import Path
from copy import deepcopy
import re

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
    doc_copy = deepcopy(doc)
    # We will modify in place below,
    # so make a deep copy so we don't surprise the caller.
    _clean(doc_copy)
    _validate(doc_copy)  # Caller will log errors.
    _translate(doc_copy)
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


_enums = load_yaml(
        (_data_dir / 'definitions.yaml').read_text()
    )['enums']


_organ_map = {
    k: re.sub(r'\s+\d+$', '', v['description'])
    for k, v in _enums['organ_types'].items()
}


def _get_schema(doc):
    entity_type = doc['entity_type'].lower()
    return _schemas[entity_type]


def _validate(doc):
    jsonschema.validate(doc, _get_schema(doc))


def _translate(doc):
    _translate_organ(doc)


def _translate_organ(doc):
    '''
    >>> doc = {'organ': 'LY01'}
    >>> _translate_organ(doc)
    >>> doc
    {'organ': 'Lymph Node'}

    >>> doc = {'origin_sample': {'organ': 'RK'}}
    >>> _translate_organ(doc)
    >>> doc
    {'origin_sample': {'organ': 'Kidney (Right)'}}

    '''
    if 'organ' in doc:
        doc['organ'] = _organ_map[doc['organ']]
    if 'origin_sample' in doc:
        _translate_organ(doc['origin_sample'])
