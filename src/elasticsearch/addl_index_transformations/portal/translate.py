from pathlib import Path
import re
from datetime import datetime

from yaml import safe_load as load_yaml


class TranslationException(Exception):
    pass


def _unexpected(s):
    return f'[{s}]'


def translate(doc):
    _translate_status(doc)
    _translate_organ(doc)
    _translate_donor_metadata(doc)
    _translate_specimen_type(doc)
    _translate_data_type(doc)
    _translate_timestamp(doc)


# Utils:

_data_dir = Path(__file__).parent / 'search-schema' / 'data'


_enums = load_yaml(
    (_data_dir / 'definitions.yaml').read_text()
)['enums']


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


# Status:

def _translate_status(doc):
    '''
    >>> doc = {'status': 'NEW'}
    >>> _translate_status(doc); doc
    {'status': 'NEW', 'mapped_status': 'New'}

    >>> doc = {'status': 'qa'}
    >>> _translate_status(doc); doc
    {'status': 'qa', 'mapped_status': 'QA'}

    >>> doc = {'status': 'xyz'}
    >>> _translate_status(doc); doc
    {'status': 'xyz', 'mapped_status': '[xyz]'}

    '''
    _map(doc, 'status', _status_map)


def _status_map(k):
    k_upper = k.upper()
    # Most of the real data doesn't satisfy the spec.
    if k_upper == 'QA':
        return 'QA'
    if k_upper not in _status_dict:
        return _unexpected(k)
    description = _status_dict[k_upper]
    return description.title()


_status_dict = {
    k: v['description']
    for k, v in _enums['dataset_status_types'].items()
}


# Timestamp:

def _translate_timestamp(doc):
    '''
    >>> doc = {
    ...    'create_timestamp': '1575489509656',
    ...    'last_modified_timestamp': 1590017663118
    ... }
    >>> _translate_timestamp(doc);
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
    {'origin_sample': {'organ': 'ZZ', 'mapped_organ': '[ZZ]'}}

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
    {'specimen_type': 'xyz', 'mapped_specimen_type': '[xyz]'}

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

    >>> doc = {'data_types': ['xyz']}
    >>> _translate_data_type(doc); doc
    {'data_types': ['xyz'], 'mapped_data_types': ['[xyz]']}

    '''
    _map(doc, 'data_types', _data_types_map)


def _data_types_map(ks):
    return [
        _data_types_dict[k] if k in _data_types_dict else _unexpected(k)
        for k in ks
    ]


_data_types_dict = {
    k: v['description']
    for k, v in _enums['assay_types'].items()
    # NOTE: Field name ("data_types") and enum name ("assay_types") do not match!
}


# Donor metadata:

def _translate_donor_metadata(doc):
    '''
    >>> doc = {"metadata": "Not a dict!"}
    >>> _translate_donor_metadata(doc)
    >>> doc
    {'metadata': 'Not a dict!', 'mapped_metadata': {}}

    >>> doc = {
    ...     "metadata": {
    ...         "organ_donor_data": [
    ...             {
    ...                 "data_type": "Nominal",
    ...                 "grouping_concept_preferred_term":
    ...                     "Gender finding",
    ...                 "preferred_term": "Masculine gender",
    ...             },
    ...             {
    ...                 "data_type": "Numeric",
    ...                 "data_value": "58",
    ...                 "grouping_concept_preferred_term":
    ...                     "Current chronological age",
    ...                 "units": "months"
    ...             },
    ...             {
    ...                 "data_type": "Numeric",
    ...                 "data_value": "22",
    ...                 "grouping_concept_preferred_term":
    ...                     "Body mass index",
    ...                 "units": "kg/m^17"
    ...             },
    ...             {
    ...                 "data_type": "Nominal",
    ...                 "grouping_code": "415229000",
    ...                 "grouping_concept":
    ...                     "not recognized: will fall-back to code",
    ...                 "grouping_concept_preferred_term":
    ...                     "not recognized: will fall-back to code",
    ...                 "preferred_term": "African race",
    ...             }
    ...         ]
    ...     }
    ... }
    >>> _translate_donor_metadata(doc)
    >>> len(doc['metadata']['organ_donor_data'])
    4
    >>> from pprint import pprint
    >>> pprint(doc['mapped_metadata'])
    {'age': 4.8, 'bmi': 22.0, 'gender': 'Masculine gender', 'race': 'African race'}

    >>> doc = {
    ...     "metadata": {
    ...         "organ_donor_data": [{
    ...             "grouping_code": "BAD",
    ...             "grouping_concept": "BAD",
    ...             "grouping_concept_preferred_term": "BAD"
    ...         }]
    ...     }
    ... }
    >>> _translate_donor_metadata(doc)
    Traceback (most recent call last):
    ...
    translate.TranslationException: Unexpected grouping: {'grouping_code': 'BAD', 'grouping_concept': 'BAD', 'grouping_concept_preferred_term': 'BAD'}

    '''
    _map(doc, 'metadata', _donor_metadata_map)


def _donor_metadata_map(metadata):
    AGE = 'age'
    BMI = 'bmi'
    GENDER = 'gender'
    RACE = 'race'
    # I'm just not sure which one of these will be stable
    # if the preferred vocabulary changes.
    # If the vocabulary is stable, this can be simplified!
    grouping_terms = {
        'Body mass index': BMI,
        'Current chronological age': AGE,
        'Gender finding': GENDER,
        'Racial group': RACE
    }
    grouping_concepts = {
        'C1305855': BMI,
        'C0001779': AGE,
        'C1287419': GENDER,
        'C0027567': RACE
    }
    grouping_codes = {
        '60621009': BMI,
        '424144002': AGE,
        '365873007': GENDER,
        '415229000': RACE
    }
    mapped_metadata = {}
    if isinstance(metadata, dict) and 'organ_donor_data' in metadata:
        for kv in metadata['organ_donor_data']:
            k = (
                (kv['grouping_concept_preferred_term'] in grouping_terms
                    and grouping_terms[kv['grouping_concept_preferred_term']])
                or (kv['grouping_concept'] in grouping_concepts
                    and grouping_concepts[kv['grouping_concept']])
                or (kv['grouping_code'] in grouping_codes
                    and grouping_codes[kv['grouping_code']])
            )
            if not k:
                raise TranslationException(f'Unexpected grouping: {kv}')
            if k == AGE and kv['units'] == 'months':
                v = round(float(kv['data_value']) / 12, 1)
            else:
                v = (
                    kv['preferred_term']
                    if kv['data_type'] == 'Nominal'
                    else float(kv['data_value'])
                )
            mapped_metadata[k] = v
    return mapped_metadata
