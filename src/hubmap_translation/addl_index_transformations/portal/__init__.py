import argparse
from pathlib import Path
from copy import deepcopy
import logging
from json import dumps
import datetime

from yaml import safe_load as load_yaml
import jsonschema

from hubmap_translation.addl_index_transformations.portal.translate import (
    translate, TranslationException
)
from hubmap_translation.addl_index_transformations.portal.add_counts import (
    add_counts
)
from hubmap_translation.addl_index_transformations.portal.add_partonomy import (
    add_partonomy
)
from hubmap_translation.addl_index_transformations.portal.sort_files import (
    sort_files
)
from hubmap_translation.addl_index_transformations.portal.reset_entity_type import (
    reset_entity_type
)

from hubmap_translation.addl_index_transformations.portal.add_assay_details import (
    add_assay_details
)

from hubmap_translation.addl_index_transformations.portal.lift_dataset_metadata_fields import (
    lift_dataset_metadata_fields
)

from hubmap_translation.addl_index_transformations.portal.get_organ_map import (
    get_organ_map
)


def _get_version():
    # Use the generated BUILD (under project root directory) version (git branch name:short commit hash)
    # as Elasticsearch mapper_metadata.version
    build_path = Path(__file__).absolute(
    ).parent.parent.parent.parent.parent / 'BUILD'
    if build_path.is_file():
        # Use strip() to remove leading and trailing spaces, newlines, and tabs
        version = build_path.read_text().strip()
        logging.debug(f'Read "{version}" from {build_path}')
        return version
    logging.debug(f'Using place-holder version; No such file: {build_path}')
    return 'no-build-file'


def get_config():
    '''
    >>> es_config = get_config()
    >>> print(list(es_config.keys()))
    ['settings', 'mappings']

    '''
    return load_yaml((Path(__file__).parent / 'config.yaml').read_text())


def transform(doc, transformation_resources, batch_id='unspecified'):
    id_for_log = f'Batch {batch_id}; UUID {doc["uuid"] if "uuid" in doc else "missing"}'
    logging.info(f'Begin: {id_for_log}')
    doc_copy = deepcopy(doc)
    # We will modify in place below,
    # so make a deep copy so we don't surprise the caller.
    _add_validation_errors(doc_copy)
    _clean(doc_copy)
    doc_copy['transformation_errors'] = []
    organ_map = get_organ_map(transformation_resources)
    try:
        add_assay_details(doc_copy, transformation_resources)
        lift_dataset_metadata_fields(doc_copy)
        translate(doc_copy, organ_map)
    except TranslationException as e:
        logging.error(f'Error: {id_for_log}: {e}')
        return None
    sort_files(doc_copy)
    add_counts(doc_copy)
    add_partonomy(doc_copy, organ_map)
    reset_entity_type(doc_copy)
    if len(doc_copy['transformation_errors']) == 0:
        del doc_copy['transformation_errors']
    doc_copy['mapper_metadata'].update({
        'version': _get_version(),
        'datetime': str(datetime.datetime.now()),
        'size': len(dumps(doc_copy))
    })
    logging.info(f'End: {id_for_log}')
    return doc_copy


def _clean(doc):
    _map(doc, _simple_clean)


def _map(doc, clean):
    # The recursion is usually not needed...
    # but better to do it everywhere than to miss one case.
    clean(doc)

    single_valued_fields = ['donor', 'rui_location']
    multi_valued_fields = ['ancestors', 'descendants',
                           'immediate_ancestors', 'immediate_descendants']

    for single_doc_field in single_valued_fields:
        if single_doc_field in doc:
            fragment = doc[single_doc_field]
            _map(fragment, clean)
    for multi_doc_field in multi_valued_fields:
        if multi_doc_field in doc:
            for fragment in doc[multi_doc_field]:
                _map(fragment, clean)


def _simple_clean(doc):
    # We shouldn't get messy data in the first place...
    # but it's just not feasible to make the fixes upstream.
    if not isinstance(doc, dict):
        return

    # Clean up names conservatively,
    # based only on the problems we actually see:
    name_field = 'created_by_user_displayname'
    if doc.get(name_field, '').lower() in [
            'daniel cotter', 'amir bahmani', 'adam kagel', 'gloria pryhuber']:
        doc[name_field] = doc[name_field].title()

    # Clean up metadata:
    if 'metadata' in doc and 'metadata' in doc['metadata']:
        metadata = doc['metadata']['metadata']

        bad_fields = [
            'collectiontype', 'null',  # Inserted by IEC.
            # Only meaningful at submission time.
            'data_path', 'metadata_path', 'version',
            'donor_id', 'tissue_id'  # For internal use only.
        ]

        # Ideally, we'd pull from https://github.com/hubmapconsortium/ingest-validation-tools/blob/main/docs/field-types.yaml
        # here, or make the TSV parsing upstream schema aware,
        # instead of trying to guess, but I think the number of special cases will be relatively small.
        not_really_a_number = ['cell_barcode_size', 'cell_barcode_offset']

        # Explicitly convert items to list,
        # so we can remove keys from the metadata dict:
        for k, v in list(metadata.items()):
            if k in bad_fields or k.startswith('_'):
                del metadata[k]
                continue

            # Normalize booleans to all-caps, the Excel default.
            # (There is no guaratee that boolean fields with be prefixed this way,
            # but at the moment it is the case.)
            if k.startswith('is_'):
                if v in ['0', 'false', 'False']:
                    metadata[k] = 'FALSE'
                if v in ['1', 'true', 'True']:
                    metadata[k] = 'TRUE'
                continue

            if k not in not_really_a_number:
                try:
                    as_number = int(v)
                except ValueError:
                    try:
                        as_number = float(v)
                    except ValueError:
                        as_number = None
                if as_number is not None:
                    metadata[k] = as_number


# TODO: Reenable this when we have time, and can make sure we don't need these fields.
#
#     schema = _get_schema(doc)
#     allowed_props = schema['properties'].keys()
#     keys = list(doc.keys())
#     for key in keys:
#         if key not in allowed_props:
#             del doc[key]

#     # Not used in portal:
#     for unused_key in [
#         'ancestors',  # ancestor_ids *is* used in portal.
#         'descendants',
#         'descendant_ids',
#         'hubmap_display_id',  # Only used in ingest.
#         'rui_location'
#     ]:
#         if unused_key in doc:
#             del doc[unused_key]


def _get_schema(doc):
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "uuid": {
                "type": "string",
                "pattern": "^[0-9a-f]{32}$"
            }
        },
        "required": ["uuid"]
    }


def _add_validation_errors(doc):
    '''
    >>> from pprint import pprint

    >>> doc = {}
    >>> _add_validation_errors(doc)
    >>> pprint(doc['mapper_metadata']['validation_errors'])
    [{'absolute_path': '/',
      'absolute_schema_path': '/required',
      'message': "'uuid' is a required property"}]

    >>> doc = {
    ...    'uuid': 'not-uuid',
    ... }
    >>> _add_validation_errors(doc)
    >>> pprint(doc['mapper_metadata']['validation_errors'])
    [{'absolute_path': '/uuid',
      'absolute_schema_path': '/properties/uuid/pattern',
      'message': "'not-uuid' does not match '^[0-9a-f]{32}$'"}]

    >>> doc = {
    ...    'uuid': '0123456789abcdef0123456789abcdef',
    ... }
    >>> _add_validation_errors(doc)
    >>> pprint(doc['mapper_metadata']['validation_errors'])
    []

    '''
    schema = _get_schema(doc)
    validator = jsonschema.Draft7Validator(schema)
    errors = [
        {
            'message': e.message,
            'absolute_schema_path': _as_path_string(e.absolute_schema_path),
            'absolute_path': _as_path_string(e.absolute_path)
        } for e in validator.iter_errors(doc)
    ]
    doc['mapper_metadata'] = {'validation_errors': errors}


def _as_path_string(mixed):
    '''
    >>> _as_path_string(['a', 2, 'z'])
    '/a/2/z'

    '''
    sep = '/'
    return sep + sep.join(str(s) for s in mixed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Given a source document, transform it.'
    )

    parser.add_argument('input', type=argparse.FileType(
        'r'), help='Path of input YAML/JSON.')
    args = parser.parse_args()
    input_yaml = args.input.read()
    doc = load_yaml(input_yaml)
    transformed = transform(doc)
    print(dumps(transformed, sort_keys=True, indent=2))
