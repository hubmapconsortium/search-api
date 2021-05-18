from pathlib import Path
import re
from datetime import datetime
from collections import defaultdict

from yaml import safe_load as load_yaml

from libs.assay_type import AssayType


class TranslationException(Exception):
    pass


def _unexpected(s):
    return f'No translation for "{s}"'


def translate(doc):
    _add_metadata_metadata_placeholder(doc)
    _translate_status(doc)
    _translate_organ(doc)
    _translate_donor_metadata(doc)
    _translate_specimen_type(doc)
    _translate_data_type(doc)
    _translate_timestamp(doc)
    _translate_access_level(doc)


# Utils:

_enums_dir = Path(__file__).parent.parent.parent.parent / 'search-schema' / 'data' / 'definitions' / 'enums'
_enums = {path.stem: load_yaml(path.read_text()) for path in _enums_dir.iterdir()}


def _map(doc, key, map):
    # The recursion is usually not needed...
    # but better to do it everywhere than to miss one case.
    if key in doc:
        doc[f'mapped_{key}'] = map(doc[key])
    if 'donor' in doc:
        _map(doc['donor'], key, map)
    if 'origin_sample' in doc:
        _map(doc['origin_sample'], key, map)
    if 'source_sample' in doc:
        for sample in doc['source_sample']:
            _map(sample, key, map)
    if 'ancestors' in doc:
        for ancestor in doc['ancestors']:
            _map(ancestor, key, map)


def _add_metadata_metadata_placeholder(doc):
    '''
    For datasets, the "metadata" used by the portal is actually at
    "metadata.metadata" and in dev-search, there is a boolean facet
    that looks for this path. Samples and Donors don't follow this pattern,
    but to enable the boolean facet, we add a placeholder.

    >>> doc = {'entity_type': 'Donor', 'metadata': {}}
    >>> _add_metadata_metadata_placeholder(doc)
    >>> assert 'metadata' in doc['metadata']

    >>> doc = {'entity_type': 'Donor'}
    >>> _add_metadata_metadata_placeholder(doc)
    >>> assert 'metadata' not in doc

    >>> doc = {'entity_type': 'Dataset', 'metadata': {}}
    >>> _add_metadata_metadata_placeholder(doc)
    >>> assert 'metadata' not in doc['metadata']

    '''
    if doc['entity_type'] in ['Donor', 'Sample'] and 'metadata' in doc:
        doc['metadata']['metadata'] = {'has_metadata': True}


# Data access level:

def _translate_access_level(doc):
    '''
    >>> doc = {'data_access_level': 'consortium'}
    >>> _translate_access_level(doc); doc
    {'data_access_level': 'consortium', 'mapped_data_access_level': 'Consortium'}
    >>> doc = {'data_access_level': 'top-secret'}
    >>> _translate_access_level(doc); doc
    {'data_access_level': 'top-secret', 'mapped_data_access_level': 'No translation for "top-secret"'}

    '''
    _map(doc, 'data_access_level', _access_level_map)


def _access_level_map(access_level):
    if access_level not in _enums['data_access_levels'].keys():
        return _unexpected(access_level)
    return access_level.title()


# Timestamp:

def _translate_timestamp(doc):
    '''
    >>> doc = {
    ...    'create_timestamp': '1575489509656',
    ...    'last_modified_timestamp': 1590017663118
    ... }
    >>> _translate_timestamp(doc)
    >>> from pprint import pprint
    >>> pprint(doc)
    {'create_timestamp': '1575489509656',
     'last_modified_timestamp': 1590017663118,
     'mapped_create_timestamp': '2019-12-04 19:58:29',
     'mapped_last_modified_timestamp': '2020-05-20 23:34:23'}

    '''
    _map(doc, 'create_timestamp', _timestamp_map)
    _map(doc, 'last_modified_timestamp', _timestamp_map)


def _timestamp_map(timestamp):
    return (
        datetime.utcfromtimestamp(int(timestamp) / 1000)
        .strftime('%Y-%m-%d %H:%M:%S')
    )


# Status:

def _translate_status(doc):
    '''
    >>> doc = {'status': 'New'}
    >>> _translate_status(doc); doc
    {'status': 'New', 'mapped_status': 'New'}

    >>> doc = {'status': 'Foobar'}
    >>> _translate_status(doc); doc
    {'status': 'Foobar', 'mapped_status': 'No translation for "Foobar"'}
    '''
    _map(doc, 'status', _status_map)


def _status_map(status):
    if status not in _enums['dataset_status_types'].keys():
        return _unexpected(status)
    return status


# Organ:

def _translate_organ(doc):
    '''
    >>> doc = {'organ': 'LY01'}
    >>> _translate_organ(doc); doc
    {'organ': 'LY01', 'mapped_organ': 'Lymph Node'}

    >>> doc = {'origin_sample': {'organ': 'RK'}}
    >>> _translate_organ(doc); doc
    {'origin_sample': {'organ': 'RK', 'mapped_organ': 'Kidney (Right)'}}

    >>> doc = {'origin_sample': {'organ': 'ZZ'}}
    >>> _translate_organ(doc); doc
    {'origin_sample': {'organ': 'ZZ', 'mapped_organ': 'No translation for "ZZ"'}}

    '''
    _map(doc, 'organ', _organ_map)


def _organ_map(k):
    if k not in _organ_dict:
        return _unexpected(k)
    return _organ_dict[k]


_organ_dict = {
    k: re.sub(r'\s+\d+$', '', v['description'])
    for k, v in _enums['organ_types'].items()
}


# Specimen type:

def _translate_specimen_type(doc):
    '''
    >>> doc = {'specimen_type': 'fresh_frozen_tissue'}
    >>> _translate_specimen_type(doc); doc
    {'specimen_type': 'fresh_frozen_tissue', 'mapped_specimen_type': 'Fresh frozen tissue'}

    >>> doc = {'specimen_type': 'xyz'}
    >>> _translate_specimen_type(doc); doc
    {'specimen_type': 'xyz', 'mapped_specimen_type': 'No translation for "xyz"'}

    '''
    _map(doc, 'specimen_type', _specimen_types_map)


def _specimen_types_map(k):
    if k not in _specimen_types_dict:
        return _unexpected(k)
    return _specimen_types_dict[k]


_specimen_types_dict = {
    k: v['description']
    for k, v in _enums['tissue_sample_types'].items()
}


# Assay type:

def _translate_data_type(doc):
    '''
    >>> doc = {'data_types': ['AF']}
    >>> _translate_data_type(doc); doc
    {'data_types': ['AF'], 'mapped_data_types': ['Autofluorescence Microscopy']}

    >>> doc = {'data_types': ['image_pyramid', 'AF']}
    >>> _translate_data_type(doc); doc
    {'data_types': ['image_pyramid', 'AF'], 'mapped_data_types': ['Autofluorescence Microscopy [Image Pyramid]']}

    >>> doc = {'data_types': ['salmon_rnaseq_10x_sn']}
    >>> _translate_data_type(doc); doc
    {'data_types': ['salmon_rnaseq_10x_sn'], 'mapped_data_types': ['snRNA-seq [Salmon]']}

    >>> doc = {'data_types': ['xyz', 'abc', 'image_pyramid']}
    >>> _translate_data_type(doc); doc
    {'data_types': ['xyz', 'abc', 'image_pyramid'], 'mapped_data_types': ['No translation for "abc" / No translation for "xyz" [Image Pyramid]']}

    '''
    _map(doc, 'data_types', _data_types_map)


def _data_types_map(ks):
    assert len(ks) == 1 or (len(ks) == 2 and 'image_pyramid' in ks)
    single_key = ks[0] if len(ks) == 1 else ks
    try:
        r = AssayType(single_key).description
    except RuntimeError:
        if isinstance(single_key, list):
            r = _unexpected(' / '.join(single_key))
        else:
            r = _unexpected(single_key)
    return [r]


# Donor metadata:

def _translate_donor_metadata(doc):
    '''
    >>> doc = {"metadata": "Not a dict!"}
    >>> _translate_donor_metadata(doc)
    >>> doc
    {'metadata': 'Not a dict!', 'mapped_metadata': {}}

    Multi-valued fields are supported:

    >>> doc = {
    ...     "metadata": {
    ...         "organ_donor_data": [{
    ...             "preferred_term": "Diabetes",
    ...             "grouping_concept_preferred_term": "Medical history"
    ...         },
    ...         {
    ...             "preferred_term": "Cancer",
    ...             "grouping_concept_preferred_term": "Medical history"
    ...         }]
    ...     }
    ... }
    >>> _translate_donor_metadata(doc)
    >>> doc['mapped_metadata']
    {'medical_history': ['Diabetes', 'Cancer']}

    Numeric fields are turned into floats, and units are their own field:

    >>> doc = {
    ...     "metadata": {
    ...         "organ_donor_data": [{
    ...             "data_type": "Numeric",
    ...             "data_value": "87.6",
    ...             "grouping_concept_preferred_term": "Weight",
    ...             "units": "kg"
    ...         }]
    ...     }
    ... }
    >>> _translate_donor_metadata(doc)
    >>> doc['mapped_metadata']
    {'weight_value': [87.6], 'weight_unit': ['kg']}

    '''
    _map(doc, 'metadata', _donor_metadata_map)


def _donor_metadata_map(metadata):
    mapped_metadata = defaultdict(list)
    if isinstance(metadata, dict) and 'organ_donor_data' in metadata:
        for kv in metadata['organ_donor_data']:
            term = kv['grouping_concept_preferred_term']
            key = re.sub(r'\W+', '_', term).lower()
            value = (
                float(kv['data_value'])
                if 'data_type' in kv and kv['data_type'] == 'Numeric'
                else kv['preferred_term']
            )

            if 'units' not in kv or not len(kv['units']):
                mapped_metadata[key].append(value)
                continue
            mapped_metadata[f'{key}_value'].append(value)
            mapped_metadata[f'{key}_unit'].append(kv['units'])

    return dict(mapped_metadata)
