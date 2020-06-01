#!/usr/bin/env python3

from pathlib import Path
from copy import deepcopy
import logging
import sys

# import jsonschema
from yaml import dump as dump_yaml, safe_load as load_yaml

from portal_translate import translate, TranslationException


def transform(doc, batch_id='unspecified'):
    '''
    >>> transform({})
    Traceback (most recent call last):
    ...
    KeyError: 'entity_type'

    >>> from pprint import pprint
    >>> pprint(transform({
    ...    'entity_type': 'Donor',
    ...    'create_timestamp': 1575489509656,
    ...    'created_by_user_displayname': 'xxx',
    ...    'created_by_user_email': 'xxx',
    ...    'group_name': 'xxx',
    ...    'group_uuid': 'xxx',
    ...    'last_modified_timestamp': '1575489509656',
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
     'create_timestamp': '2019-12-04 19:58:29',
     'created_by_user_displayname': 'xxx',
     'created_by_user_email': 'xxx',
     'entity_type': 'Donor',
     'group_name': 'xxx',
     'group_uuid': 'xxx',
     'last_modified_timestamp': '2019-12-04 19:58:29',
     'uuid': 'xxx'}

    '''
    doc_copy = deepcopy(doc)
    # We will modify in place below,
    # so make a deep copy so we don't surprise the caller.
    _clean(doc_copy)
    try:
        translate(doc_copy)
    except TranslationException as e:
        logging.error(f'Batch {batch_id}; UUID {doc["uuid"]}: {e}')
        return None
    return doc_copy


_data_dir = Path(__file__).parent / 'search-schema' / 'data'


def _clean(doc):
    _map(doc, _simple_clean)


def _map(doc, clean):
    # The recursion is usually not needed...
    # but better to do it everywhere than to miss one case.
    clean(doc)
    if 'donor' in doc:
        _map(doc['donor'], clean)
    if 'origin_sample' in doc:
        _map(doc['origin_sample'], clean)
    if 'source_sample' in doc:
        for sample in doc['source_sample']:
            _map(sample, clean)


def _simple_clean(doc):
    schema = _get_schema(doc)
    allowed_props = schema['properties'].keys()
    keys = list(doc.keys())
    for key in keys:
        if key not in allowed_props:
            del doc[key]

    # TODO: Clean up nested objects as well.
    # Not used in portal:
    for unused_key in [
        'ancestors',  # ancestor_ids *is* used in portal.
        'descendants',
        'descendant_ids',
        'hubmap_display_id',  # Only used in ingest.
        'rui_location'
    ]:
        if unused_key in doc:
            del doc[unused_key]


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


# TODO:
# def _validate(doc):
#     jsonschema.validate(doc, _get_schema(doc))


if __name__ == "__main__":
    for name in sys.argv[1:]:
        doc = load_yaml(Path(name).read_text())
        transformed = transform(doc)
        print(dump_yaml(transformed))
