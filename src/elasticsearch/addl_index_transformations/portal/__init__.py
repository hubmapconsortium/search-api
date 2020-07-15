#!/usr/bin/env python3

from pathlib import Path
from copy import deepcopy
import logging
import sys
from json import dumps
import datetime

# import jsonschema
from yaml import dump as dump_yaml, safe_load as load_yaml

from elasticsearch.addl_index_transformations.portal.translate import (
    translate, TranslationException
)
from elasticsearch.addl_index_transformations.portal.add_everything import (
    add_everything
)
from elasticsearch.addl_index_transformations.portal.add_counts import (
    add_counts
)


def transform(doc, batch_id='unspecified'):
    '''
    >>> from pprint import pprint
    >>> transformed = transform({
    ...    'entity_type': 'dataset',
    ...    'origin_sample': {
    ...        'organ': 'LY01'
    ...    },
    ...    'create_timestamp': 1575489509656,
    ...    'ancestor_ids': ['1234', '5678'],
    ...    'ancestors': [{
    ...        'specimen_type': 'fresh_frozen_tissue_section'
    ...    }],
    ...    'data_types': ['codex_cytokit', 'seqFish'],
    ...    'descendants': [{'entity_type': 'Sample or Dataset'}],
    ...    'donor': {
    ...        "metadata": {
    ...             "organ_donor_data": [
    ...                 {
    ...                     "data_type": "Nominal",
    ...                     "grouping_concept_preferred_term":
    ...                         "Gender finding",
    ...                     "preferred_term": "Masculine gender"
    ...                 }
    ...             ]
    ...         }
    ...    }
    ... })
    >>> del transformed['mapper_metadata']['datetime']
    >>> pprint(transformed)
    {'ancestor_counts': {'entity_type': {}},
     'ancestor_ids': ['1234', '5678'],
     'ancestors': [{'mapped_specimen_type': 'Fresh Frozen Tissue Section',
                    'specimen_type': 'fresh_frozen_tissue_section'}],
     'create_timestamp': 1575489509656,
     'data_types': ['codex_cytokit', 'seqFish'],
     'descendant_counts': {'entity_type': {'Sample or Dataset': 1}},
     'descendants': [{'entity_type': 'Sample or Dataset'}],
     'donor': {'mapped_metadata': {'gender': 'Masculine gender'},
               'metadata': {'organ_donor_data': [{'data_type': 'Nominal',
                                                  'grouping_concept_preferred_term': 'Gender '
                                                                                     'finding',
                                                  'preferred_term': 'Masculine '
                                                                    'gender'}]}},
     'entity_type': 'dataset',
     'everything': ['ensure_dynamic_mapping_is_string',
                    '1',
                    '1234',
                    '1575489509656',
                    '2019-12-04 19:58:29',
                    '5678',
                    'CODEX [Cytokit + SPRM]',
                    'Fresh Frozen Tissue Section',
                    'Gender finding',
                    'LY01',
                    'Lymph Node',
                    'Masculine gender',
                    'Nominal',
                    'Sample or Dataset',
                    'codex_cytokit',
                    'dataset',
                    'fresh_frozen_tissue_section',
                    'seqFish'],
     'mapped_create_timestamp': '2019-12-04 19:58:29',
     'mapped_data_types': ['CODEX [Cytokit + SPRM]', 'seqFish'],
     'mapper_metadata': {'size': 1141, 'version': '0.0.3'},
     'origin_sample': {'mapped_organ': 'Lymph Node', 'organ': 'LY01'}}

    '''
    id_for_log = f'Batch {batch_id}; UUID {doc["uuid"] if "uuid" in doc else "missing"}'
    logging.info(f'Begin: {id_for_log}')
    doc_copy = deepcopy(doc)
    # We will modify in place below,
    # so make a deep copy so we don't surprise the caller.
    _clean(doc_copy)
    try:
        translate(doc_copy)
    except TranslationException as e:
        logging.error(f'Error: {id_for_log}: {e}')
        return None
    add_counts(doc_copy)
    add_everything(doc_copy)
    doc_copy['mapper_metadata'] = {
        'version': '0.0.3',
        'datetime': str(datetime.datetime.now()),
        'size': len(dumps(doc_copy))
    }
    logging.info(f'End: {id_for_log}')
    return doc_copy


_data_dir = Path(__file__).parent / 'search-schema' / 'data'


def _clean(doc):
    return doc
    # TODO: Reenable.
    # _map(doc, _simple_clean)


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

# TODO: Reenable this when we have time, and can make sure we don't need these fields.
#
# def _simple_clean(doc):
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
