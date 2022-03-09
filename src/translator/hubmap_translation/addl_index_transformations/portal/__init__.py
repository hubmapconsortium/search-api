import argparse
from pathlib import Path
from copy import deepcopy
import logging
from json import dumps
import datetime
import subprocess
from tempfile import TemporaryDirectory

from yaml import safe_load as load_yaml
import jsonschema

from translator.hubmap_translation.addl_index_transformations.portal.translate import (
    translate, TranslationException
)
from translator.hubmap_translation.addl_index_transformations.portal.add_counts import (
    add_counts
)
from translator.hubmap_translation.addl_index_transformations.portal.add_partonomy import (
    add_partonomy
)
from translator.hubmap_translation.addl_index_transformations.portal.sort_files import (
    sort_files
)
from translator.hubmap_translation.addl_index_transformations.portal.reset_entity_type import (
    reset_entity_type
)


def _get_version():
    # Use the generated BUILD (under project root directory) version (git branch name:short commit hash)
    # as Elasticsearch mapper_metadata.version
    build_path = Path(__file__).absolute().parent.parent.parent.parent.parent / 'BUILD'
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


def transform(doc, batch_id='unspecified'):
    '''
    >>> from pprint import pprint
    >>> transformed = transform({
    ...    'entity_type': 'dataset',
    ...    'status': 'New',
    ...    'group_name': 'EXT - Outside HuBMAP',
    ...    'origin_sample': {
    ...        'organ': 'LY'
    ...    },
    ...    'create_timestamp': 1575489509656,
    ...    'ancestor_ids': ['1234', '5678'],
    ...    'ancestors': [{
    ...        'specimen_type': 'fresh_frozen_tissue_section',
    ...        'created_by_user_displayname': 'daniel Cotter'
    ...    }],
    ...    'data_access_level': 'consortium',
    ...    'data_types': ['salmon_rnaseq_10x_sn'],
    ...    'descendants': [{'entity_type': 'Sample or Dataset'}],
    ...    'donor': {
    ...        "metadata": {
    ...            "organ_donor_data": [
    ...                {
    ...                    "data_type": "Nominal",
    ...                    "grouping_concept_preferred_term": "Sex",
    ...                    "preferred_term": "Male"
    ...                }
    ...            ]
    ...        }
    ...    },
    ...    'files': [{
    ...        "description": "OME-TIFF pyramid file",
    ...        "edam_term": "EDAM_1.24.format_3727",
    ...        "is_qa_qc": False,
    ...        "rel_path": "ometiff-pyramids/stitched/expressions/reg1_stitched_expressions.ome.tif",
    ...        "size": 123456789,
    ...        "type": "unknown"
    ...    }],
    ...    'metadata': {
    ...        'metadata': {
    ...            '_random_stuff_that_should_not_be_ui': 'No!',
    ...            'collectiontype': 'No!',
    ...            'data_path': 'No!',
    ...            'metadata_path': 'No!',
    ...            'tissue_id': 'No!',
    ...            'donor_id': 'No!',
    ...            'cell_barcode_size': '123',
    ...            'should_be_int': '123',
    ...            'should_be_float': '123.456',
    ...            'keep_this_field': 'Yes!',
    ...            'is_boolean': '1'
    ...        }
    ...    },
    ...    'rui_location': '{"ccf_annotations": ["http://purl.obolibrary.org/obo/UBERON_0001157"]}'
    ... })
    >>> del transformed['mapper_metadata']
    >>> pprint(transformed)
    {'anatomy_0': ['body'],
     'anatomy_1': ['large intestine', 'lymph node'],
     'anatomy_2': ['transverse colon'],
     'ancestor_counts': {'entity_type': {}},
     'ancestor_ids': ['1234', '5678'],
     'ancestors': [{'created_by_user_displayname': 'Daniel Cotter',
                    'mapped_specimen_type': 'Fresh frozen tissue section',
                    'specimen_type': 'fresh_frozen_tissue_section'}],
     'create_timestamp': 1575489509656,
     'data_access_level': 'consortium',
     'data_types': ['salmon_rnaseq_10x_sn'],
     'descendant_counts': {'entity_type': {'Sample or Dataset': 1}},
     'descendants': [{'entity_type': 'Sample or Dataset'}],
     'donor': {'mapped_metadata': {'sex': ['Male']},
               'metadata': {'organ_donor_data': [{'data_type': 'Nominal',
                                                  'grouping_concept_preferred_term': 'Sex',
                                                  'preferred_term': 'Male'}]}},
     'entity_type': 'dataset',
     'files': [{'description': 'OME-TIFF pyramid file',
                'edam_term': 'EDAM_1.24.format_3727',
                'is_qa_qc': False,
                'mapped_description': 'OME-TIFF pyramid file (TIF file)',
                'rel_path': 'ometiff-pyramids/stitched/expressions/reg1_stitched_expressions.ome.tif',
                'size': 123456789,
                'type': 'unknown'}],
     'group_name': 'EXT - Outside HuBMAP',
     'mapped_consortium': 'Outside HuBMAP',
     'mapped_create_timestamp': '2019-12-04 19:58:29',
     'mapped_data_access_level': 'Consortium',
     'mapped_data_types': ['snRNA-seq [Salmon]'],
     'mapped_external_group_name': 'Outside HuBMAP',
     'mapped_metadata': {},
     'mapped_status': 'New',
     'metadata': {'metadata': {'cell_barcode_size': '123',
                               'is_boolean': 'TRUE',
                               'keep_this_field': 'Yes!',
                               'should_be_float': 123.456,
                               'should_be_int': 123}},
     'origin_sample': {'mapped_organ': 'Lymph Node', 'organ': 'LY'},
     'rui_location': '{"ccf_annotations": '
                     '["http://purl.obolibrary.org/obo/UBERON_0001157"]}',
     'status': 'New'}

    '''
    id_for_log = f'Batch {batch_id}; UUID {doc["uuid"] if "uuid" in doc else "missing"}'
    logging.info(f'Begin: {id_for_log}')
    doc_copy = deepcopy(doc)
    # We will modify in place below,
    # so make a deep copy so we don't surprise the caller.
    _add_validation_errors(doc_copy)
    _clean(doc_copy)
    try:
        translate(doc_copy)
    except TranslationException as e:
        logging.error(f'Error: {id_for_log}: {e}')
        return None
    sort_files(doc_copy)
    add_counts(doc_copy)
    add_partonomy(doc_copy)
    reset_entity_type(doc_copy)
    doc_copy['mapper_metadata'].update({
        'version': _get_version(),
        'datetime': str(datetime.datetime.now()),
        'size': len(dumps(doc_copy))
    })
    logging.info(f'End: {id_for_log}')
    return doc_copy


_data_dir = Path(__file__).parent.parent.parent.parent / 'search-schema' / 'data'


def _clean(doc):
    _map(doc, _simple_clean)


def _map(doc, clean):
    # The recursion is usually not needed...
    # but better to do it everywhere than to miss one case.
    clean(doc)

    single_valued_fields = ['donor', 'origin_sample', 'source_sample', 'rui_location']
    multi_valued_fields = ['ancestors', 'descendants', 'immediate_ancestors', 'immediate_descendants']

    for single_doc_field in single_valued_fields:
        if single_doc_field in doc:
            fragment = doc[single_doc_field]
            logging.debug(f'Mapping single "{single_doc_field}": {dumps(fragment)[:50]}...')
            _map(fragment, clean)
            logging.debug(f'... done mapping "{single_doc_field}"')
    for multi_doc_field in multi_valued_fields:
        if multi_doc_field in doc:
            for fragment in doc[multi_doc_field]:
                logging.debug(f'Mapping multi "{multi_doc_field}": {dumps(fragment)[:50]}...')
                _map(fragment, clean)
                logging.debug(f'... done mapping "{multi_doc_field}"')


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
            'data_path', 'metadata_path', 'version',  # Only meaningful at submission time.
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
    with TemporaryDirectory() as td:
        # Regenerating schemas with every validation is wasteful.
        # Chuck would strongly prefer that this be cached.
        script_path = _data_dir.parent.parent / 'generate-schemas.sh'
        subprocess.run([script_path, td], check=True)
        entity_type = doc['entity_type'].lower()
        schema_path = Path(td) / f'{entity_type}.schema.yaml'
        schema = load_yaml(schema_path.read_text())
    return schema


def _add_validation_errors(doc):
    '''
    >>> from pprint import pprint

    >>> doc = {'entity_type': 'JUST WRONG'}
    >>> try:
    ...     _add_validation_errors(doc)
    ... except FileNotFoundError as e:
    ...     assert 'just wrong.schema.yaml' in str(e)

    >>> doc = {'entity_type': 'dataset'}
    >>> _add_validation_errors(doc)
    >>> pprint(doc['mapper_metadata']['validation_errors'][0])
    {'absolute_path': '/entity_type',
     'absolute_schema_path': '/properties/entity_type/enum',
     'message': "'dataset' is not one of ['Collection', 'Dataset', 'Donor', "
                "'Sample']"}

    >>> doc = {
    ...    'entity_type': 'Donor',
    ...    'create_timestamp': 'FAKE',
    ...    'created_by_user_displayname': 'FAKE',
    ...    'created_by_user_email': 'FAKE',
    ...    'data_access_level': 'public',
    ...    'group_name': 'FAKE',
    ...    'group_uuid': 'FAKE',
    ...    'last_modified_timestamp': 'FAKE',
    ...    'uuid': 'FAKE',
    ...    'access_group': 'FAKE',
    ...    'ancestor_ids': 'FAKE',
    ...    'ancestors': 'FAKE',
    ...    'descendant_ids': 'FAKE',
    ...    'descendants': 'FAKE'
    ... }
    >>> _add_validation_errors(doc)
    >>> pprint(doc['mapper_metadata']['validation_errors'])
    []

    '''
    schema = _get_schema(doc)
    if not schema.keys():
        doc['mapper_metadata'] = {'validation_errors': ["Can't load schema"]}
        return
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

    parser.add_argument('input', type=argparse.FileType('r'), help='Path of input YAML/JSON.')
    args = parser.parse_args()
    input_yaml = args.input.read()
    doc = load_yaml(input_yaml)
    transformed = transform(doc)
    print(dumps(transformed, sort_keys=True, indent=2))
