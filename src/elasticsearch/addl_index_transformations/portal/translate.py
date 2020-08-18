from pathlib import Path
import re
from datetime import datetime

from yaml import safe_load as load_yaml


class TranslationException(Exception):
    pass


def _unexpected(s):
    return f'{{{s}}}'


def translate(doc):
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


# Data access level:

def _translate_access_level(doc):
    '''
    >>> doc = {'data_access_level': 'consortium'}
    >>> _translate_access_level(doc); doc
    {'data_access_level': 'consortium', 'mapped_data_access_level': 'Consortium'}
    >>> doc = {'data_access_level': 'top-secret'}
    >>> _translate_access_level(doc); doc
    {'data_access_level': 'top-secret', 'mapped_data_access_level': '{top-secret}'}

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
    {'status': 'Foobar', 'mapped_status': '{Foobar}'}
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
    {'origin_sample': {'organ': 'ZZ', 'mapped_organ': '{ZZ}'}}

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
    {'specimen_type': 'xyz', 'mapped_specimen_type': '{xyz}'}

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

    >>> doc = {'data_types': ['xyz', 'abc', 'image_pyramid']}
    >>> _translate_data_type(doc); doc
    {'data_types': ['xyz', 'abc', 'image_pyramid'], 'mapped_data_types': ['{abc} / {xyz} [Image Pyramid]']}

    '''
    _map(doc, 'data_types', _data_types_map)


def _data_types_map(ks):
    pyramid_key = 'image_pyramid'
    types = ' / '.join(sorted([
        _data_types_dict[k] if k in _data_types_dict else _unexpected(k)
        for k in ks if k != pyramid_key
    ]))
    if pyramid_key in ks:
        types = f'{types} [{_data_types_dict[pyramid_key]}]'
    return [types]  # Downstream code expects to see an array.


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
    ...                 "grouping_code": "365873007",
    ...                 "grouping_concept_preferred_term":
    ...                     "Gender finding",
    ...                 "preferred_term": "Masculine gender",
    ...             },
    ...             {
    ...                 "data_type": "Numeric",
    ...                 "data_value": "58",
    ...                 "grouping_code": "424144002",
    ...                 "grouping_concept_preferred_term":
    ...                     "Current chronological age",
    ...                 "units": "months"
    ...             },
    ...             {
    ...                 "data_type": "Numeric",
    ...                 "data_value": "22",
    ...                 "grouping_code": "60621009",
    ...                 "grouping_concept_preferred_term":
    ...                     "Body mass index",
    ...                 "units": "kg/m^17"
    ...             },
    ...             {
    ...                 "data_type": "Nominal",
    ...                 "grouping_code": "415229000",
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
    {'age': 4.8,
     'bmi': 22.0,
     'gender': 'Masculine gender',
     'race': 'African race',
     'sex': 'Masculine gender'}

    >>> doc = {
    ...     "metadata": {
    ...         "organ_donor_data": [
    ...             {
    ...                 "data_type": "Nominal",
    ...                 "preferred_term": "Male",
    ...                 "grouping_concept": "C1522384",
    ...                 "grouping_concept_preferred_term": "Sex",
    ...                 "grouping_code": "57312000",
    ...             }
    ...         ]
    ...     }
    ... }
    >>> _translate_donor_metadata(doc)
    >>> pprint(doc['mapped_metadata'])
    {'gender': 'Male', 'sex': 'Male'}

    >>> doc = {
    ...     "metadata": {
    ...         "organ_donor_data": [{
    ...             "preferred_term": "Diabetes",
    ...             "grouping_code": "UNKNOWN",
    ...             "grouping_concept_preferred_term": "Medical history ... or anything else"
    ...         }]
    ...     }
    ... }
    >>> _translate_donor_metadata(doc)
    >>> pprint(doc['mapped_metadata'])
    {'medical_history_or_anything_else': ['Diabetes']}

    '''
    _map(doc, 'metadata', _donor_metadata_map)


def _donor_metadata_map(metadata):
    AGE = 'age'
    BMI = 'bmi'
    GENDER = 'gender'
    SEX = 'sex'
    RACE = 'race'
    # The "grouping_codes" seem to be the most stable,
    # by "grouping_concepts" or "grouping_terms" could also be used.
    grouping_codes = {
        '60621009': BMI,
        '424144002': AGE,
        '365873007': GENDER,
        '57312000': SEX,
        '415229000': RACE
    }
    mapped_metadata = {}
    if isinstance(metadata, dict) and 'organ_donor_data' in metadata:
        for kv in metadata['organ_donor_data']:
            if not kv['grouping_code'] in grouping_codes:
                # NOTE: This branch shouldn't be used on a regular basis:
                # Using a grouping_code makes it more robust if the
                # grouping_concept_preferred_term changes.
                # TODO: I see that some of the new fields are multi-valued.
                # Perhaps make all donor metadata arrays for consistency?
                normed = re.sub(r'\W+', '_', kv['grouping_concept_preferred_term']).lower()
                if normed in mapped_metadata:
                    mapped_metadata[normed].append(kv['preferred_term'])
                else:
                    mapped_metadata[normed] = [kv['preferred_term']]
                continue
            k = grouping_codes[kv['grouping_code']]
            if k == AGE and kv['units'] == 'months':
                v = round(float(kv['data_value']) / 12, 1)
            else:
                v = (
                    kv['preferred_term']
                    if kv['data_type'] == 'Nominal'
                    else float(kv['data_value'])
                )
            if k == SEX:
                # TODO: When the UI is caught up, only use sex.
                mapped_metadata[GENDER] = v
            elif k == GENDER and SEX not in mapped_metadata:
                # If we still have old donor metadata, we can move the UI forward
                mapped_metadata[SEX] = v
            mapped_metadata[k] = v
    return mapped_metadata
