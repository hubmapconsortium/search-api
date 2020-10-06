from pathlib import Path
from copy import deepcopy
import logging
from json import dumps
import datetime
import subprocess

from yaml import safe_load as load_yaml
import jsonschema

from elasticsearch.addl_index_transformations.portal.translate import (
    translate, TranslationException
)
from elasticsearch.addl_index_transformations.portal.add_everything import (
    add_everything, single_valued_fields, multi_valued_fields
)
from elasticsearch.addl_index_transformations.portal.add_counts import (
    add_counts
)
from elasticsearch.addl_index_transformations.portal.sort_files import (
    sort_files
)


def _get_version():
    # Use the generated BUILD (under root directory) version (git branch name:short commit hash)
    # as Elasticsearch mapper_metadata.version
    build_path = Path(__file__).parent.parent.parent.parent.parent / 'BUILD'
    if build_path.is_file():
        # Use strip() to remove leading and trailing spaces, newlines, and tabs
        version = build_path.read_text().strip()
        logging.debug(f'Read "{version}" from {build_path}')
        return version
    logging.debug(f'Using place-holder version; No such file: {build_path}')
    return 'no-build-file'


def transform(doc, batch_id='unspecified'):
    '''
    >>> from pprint import pprint
    >>> transformed = transform({
    ...    'entity_type': 'dataset',
    ...    'status': 'New',
    ...    'origin_sample': {
    ...        'organ': 'LY01'
    ...    },
    ...    'create_timestamp': 1575489509656,
    ...    'ancestor_ids': ['1234', '5678'],
    ...    'ancestors': [{
    ...        'specimen_type': 'fresh_frozen_tissue_section',
    ...        'created_by_user_displayname': 'daniel Cotter'
    ...    }],
    ...    'data_access_level': 'consortium',
    ...    'data_types': ['codex_cytokit', 'seqFish'],
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
    ...    'metadata': {
    ...        'metadata': {
    ...            '_random_stuff_that_should_not_be_ui': True,
    ...            'unrealistic': 'Donors do not have metadata/metadata.'
    ...        }
    ...    }
    ... })
    >>> del transformed['mapper_metadata']['datetime']
    >>> del transformed['mapper_metadata']['version']
    >>> del transformed['mapper_metadata']['validation_errors']
    >>> pprint(transformed)
    {'ancestor_counts': {'entity_type': {}},
     'ancestor_ids': ['1234', '5678'],
     'ancestors': [{'created_by_user_displayname': 'Daniel Cotter',
                    'mapped_specimen_type': 'Fresh Frozen Tissue Section',
                    'specimen_type': 'fresh_frozen_tissue_section'}],
     'create_timestamp': 1575489509656,
     'data_access_level': 'consortium',
     'data_types': ['codex_cytokit', 'seqFish'],
     'descendant_counts': {'entity_type': {'Sample or Dataset': 1}},
     'descendants': [{'entity_type': 'Sample or Dataset'}],
     'donor': {'mapped_metadata': {'sex': ['Male']},
               'metadata': {'organ_donor_data': [{'data_type': 'Nominal',
                                                  'grouping_concept_preferred_term': 'Sex',
                                                  'preferred_term': 'Male'}]}},
     'entity_type': 'dataset',
     'everything': ['1',
                    '1234',
                    '1575489509656',
                    '2019-12-04 19:58:29',
                    '5678',
                    'CODEX [Cytokit + SPRM] / seqFISH',
                    'Consortium',
                    'Donors do not have metadata/metadata.',
                    'New',
                    'codex_cytokit',
                    'consortium',
                    'dataset',
                    'seqFish'],
     'mapped_create_timestamp': '2019-12-04 19:58:29',
     'mapped_data_access_level': 'Consortium',
     'mapped_data_types': ['CODEX [Cytokit + SPRM] / seqFISH'],
     'mapped_metadata': {},
     'mapped_status': 'New',
     'mapper_metadata': {'size': 6041},
     'metadata': {'metadata': {'unrealistic': 'Donors do not have '
                                              'metadata/metadata.'}},
     'origin_sample': {'mapped_organ': 'Lymph Node', 'organ': 'LY01'},
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
    add_everything(doc_copy)
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
    field = 'created_by_user_displayname'
    if field in doc and doc[field] == 'daniel Cotter':
        doc[field] = 'Daniel Cotter'
    if field in doc and doc[field] == 'amir Bahmani':
        doc[field] = 'Amir Bahmani'

    if 'metadata' in doc and 'metadata' in doc['metadata']:
        underscores = [
            k for k in doc['metadata']['metadata'].keys()
            if k.startswith('_')
        ]
        for k in underscores:
            del doc['metadata']['metadata'][k]

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
    entity_type = doc['entity_type'].lower()
    schema_path = _data_dir / 'generated' / f'{entity_type}.schema.yaml'
    if not schema_path.exists():
        # TODO: Doing this in python is preferable to subprocess!
        logging.debug(f'Schema not available; will be built: {schema_path.resolve()}')
        script_path = _data_dir.parent / 'generate-schemas.sh'
        subprocess.run([script_path], check=True)
    schema = load_yaml(schema_path.read_text())
    return schema


def _add_validation_errors(doc):
    '''
    >>> from pprint import pprint

    >>> doc = {'entity_type': 'JUST WRONG'}
    >>> _add_validation_errors(doc)
    Traceback (most recent call last):
    ...
    FileNotFoundError: [Errno 2] No such file or directory: 'search-schema/data/generated/just wrong.schema.yaml'

    >>> doc = {'entity_type': 'dataset'}
    >>> _add_validation_errors(doc)
    >>> pprint(doc['mapper_metadata']['validation_errors'][0])
    {'absolute_path': '/entity_type',
     'absolute_schema_path': '/properties/entity_type/enum',
     'message': "'dataset' is not one of ['Dataset', 'Donor', 'Sample']"}

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
