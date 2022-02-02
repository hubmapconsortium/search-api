from pathlib import Path
import re
from datetime import datetime
from collections import defaultdict

from yaml import safe_load as load_yaml

from libs.assay_type import AssayType

from .vitessce_conf_builder.builder_factory import get_view_config_builder


class TranslationException(Exception):
    pass


def _unexpected(s):
    return f'No translation for "{s}"'


def translate(doc):
    _add_metadata_metadata_placeholder(doc)
    _translate_file_description(doc)
    _translate_status(doc)
    _translate_organ(doc)
    _translate_donor_metadata(doc)
    _translate_specimen_type(doc)
    _translate_data_type(doc)
    _translate_timestamp(doc)
    _translate_access_level(doc)
    _translate_external_consortium(doc)
    _add_vitessce_conf(doc)


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


# File description:

def _translate_file_description(doc):
    '''
    >>> doc = {'files': [{
    ...     "description": "OME-TIFF pyramid file",
    ...     "edam_term": "EDAM_1.24.format_3727",
    ...     "is_qa_qc": False,
    ...     "rel_path": "ometiff-pyramids/stitched/expressions/reg1_stitched_expressions.ome.tif",
    ...     "size": 123456789,
    ...     "type": "unknown"
    ... }]}
    >>> _translate_file_description(doc)
    >>> from pprint import pprint
    >>> pprint(doc)
    {'files': [{'description': 'OME-TIFF pyramid file',
                'edam_term': 'EDAM_1.24.format_3727',
                'is_qa_qc': False,
                'mapped_description': 'OME-TIFF pyramid file (TIF file)',
                'rel_path': 'ometiff-pyramids/stitched/expressions/reg1_stitched_expressions.ome.tif',
                'size': 123456789,
                'type': 'unknown'}]}
    '''
    for file in doc.get('files', []):
        extension = file['rel_path'].split('.')[-1].upper()
        file['mapped_description'] = file['description'] + f' ({extension} file)'


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


# External consortium:

def _translate_external_consortium(doc):
    '''
    >>> doc = {}
    >>> _translate_external_consortium(doc); doc
    {'mapped_consortium': 'HuBMAP'}

    >>> doc = {'group_name': 'Inside HuBMAP'}
    >>> _translate_external_consortium(doc); doc
    {'group_name': 'Inside HuBMAP', 'mapped_consortium': 'HuBMAP'}

    >>> doc = {'group_name': 'EXT - Outside HuBMAP'}
    >>> _translate_external_consortium(doc); doc
    {'group_name': 'EXT - Outside HuBMAP', 'mapped_external_group_name': 'Outside HuBMAP', 'mapped_consortium': 'Outside HuBMAP'}

    '''
    group_name = doc.get('group_name')
    if group_name is not None and 'EXT' in group_name:
        mapped_consortium = group_name.replace('EXT - ', '')
        doc['mapped_external_group_name'] = mapped_consortium
    else:
        mapped_consortium = 'HuBMAP'
    doc['mapped_consortium'] = mapped_consortium


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
    k: v['description']
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

    >>> doc = {'data_types': ['xyz', 'image_pyramid']}
    >>> _translate_data_type(doc); doc
    {'data_types': ['xyz', 'image_pyramid'], 'mapped_data_types': ['No translation for "[\\'xyz\\', \\'image_pyramid\\']"']}

    '''
    _map(doc, 'data_types', _data_types_map)


def _data_types_map(ks):
    assert len(ks) == 1 or (len(ks) == 2 and ('image_pyramid' in ks or 'Image Pyramid' in ks)), \
        f"Maximum 2 types, and one should be image pyramid: {ks}"
    single_key = ks[0] if len(ks) == 1 else ks
    try:
        r = AssayType(single_key).description
    except RuntimeError:
        r = _unexpected(single_key)
    return [r]


# Donor metadata:

def _translate_donor_metadata(doc):
    '''
    >>> doc = {"metadata": None}
    >>> _translate_donor_metadata(doc)
    >>> doc
    {'metadata': None, 'mapped_metadata': {}}

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
    if metadata is None:
        return {}
    donor_metadata = metadata.get('organ_donor_data') or metadata.get('living_donor_data') or {}
    mapped_metadata = defaultdict(list)

    for kv in donor_metadata:
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


# Vitessce conf

def _add_vitessce_conf(doc):
    def get_assay(name):
        return AssayType(name)
    Builder = get_view_config_builder(entity=doc, get_assay=get_assay)
    builder = Builder(doc, 'REPLACE_WITH_GROUPS_TOKEN')
    doc['vitessce'] = builder.get_conf_cells().conf
