import re
from datetime import datetime
from collections import defaultdict


class TranslationException(Exception):
    pass


def _unexpected(s):
    return f'No translation for "{s}"'


def translate(doc, organ_map):
    _add_metadata_metadata_placeholder(doc)
    _translate_file_description(doc)
    _translate_status(doc)
    _translate_organ(doc, organ_map)
    # _add_origin_samples_unique_mapped_organs depends on the existence of the mapped_organ field and must be performed after _translate_organ.
    _add_origin_samples_unique_mapped_organs(doc)
    _translate_donor_metadata(doc)
    _translate_sample_category(doc)
    _translate_timestamp(doc)
    _translate_access_level(doc)
    _translate_external_consortium(doc)
    _add_spatial_info(doc)


def _map(doc, key, map):
    # The recursion is usually not needed...
    # but better to do it everywhere than to miss one case.
    if key in doc:
        doc[f'mapped_{key}'] = map(doc[key])
    if 'donor' in doc:
        _map(doc['donor'], key, map)
    if 'origin_samples' in doc:
        for sample in doc['origin_samples']:
            _map(sample, key, map)
    if 'source_samples' in doc:
        for sample in doc['source_samples']:
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
        file['mapped_description'] = file['description'] + \
            f' ({extension} file)'


# Data access level:

def _translate_access_level(doc):
    '''
    >>> doc = {'data_access_level': 'consortium'}
    >>> _translate_access_level(doc); doc
    {'data_access_level': 'consortium', 'mapped_data_access_level': 'Consortium'}
    '''
    _map(doc, 'data_access_level', _access_level_map)


def _access_level_map(access_level):
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
    '''
    _map(doc, 'status', _status_map)


def _status_map(status):
    return status


# Organ:

def _translate_organ(doc, organ_map):
    '''
    >>> doc = {'origin_samples': [{'organ': 'RK'}]}
    >>> organ_map = {'RK': {'term': 'Kidney (Right)'}}
    >>> _translate_organ(doc, {'RK': {'term': 'Kidney (Right)'}}); doc
    {'origin_samples': [{'organ': 'RK', 'mapped_organ': 'Kidney (Right)'}]}

    >>> doc = {'origin_samples': [{'organ': 'ZZ'}]}
    >>> _translate_organ(doc, {}); doc
    {'origin_samples': [{'organ': 'ZZ', 'mapped_organ': 'No translation for "ZZ"'}]}

    '''
    def _organ_map(k):
        if k not in organ_map:
            return _unexpected(k)
        return organ_map.get(k, {}).get('term')
    _map(doc, 'organ', _organ_map)


# Sample category:

def _translate_sample_category(doc):
    '''
    >>> doc = {'sample_category': 'block'}
    >>> _translate_sample_category(doc); doc
    {'sample_category': 'block', 'mapped_sample_category': 'Block'}
    '''
    _map(doc, 'sample_category', _sample_categories_map)


def _sample_categories_map(k):
    return k.title()


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
    donor_metadata = metadata.get(
        'organ_donor_data') or metadata.get('living_donor_data') or {}
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


def _get_unique_mapped_organs(samples):
    '''
    >>> samples = [{'mapped_organ': 'Lymph Node'}, {'mapped_organ': 'Small Intestine'}, {'mapped_organ': 'Lymph Node'}]
    >>> sorted(_get_unique_mapped_organs(samples));
    ['Lymph Node', 'Small Intestine']
    '''
    return list({sample['mapped_organ'] for sample in samples if 'mapped_organ' in sample})


def _add_origin_samples_unique_mapped_organs(doc):
    if doc['entity_type'] in ['Sample', 'Dataset'] and 'origin_samples' in doc:
        doc['origin_samples_unique_mapped_organs'] = _get_unique_mapped_organs(
            doc['origin_samples'])


def _add_spatial_info(doc):
    '''
    Add a boolean field "is_spatial" to the document based on the entity type and the presence of an rui_location field.

    For samples, the is_spatial field is set to True if the rui_location field is present.
    >>> doc = {'entity_type': 'Sample', 'rui_location': 'https://example.com'}
    >>> _add_spatial_info(doc); doc['is_spatial']
    True

    For datasets, the is_spatial field is set to True if any ancestor has an rui_location field.
    The rui_location field is also copied from the nearest ancestor with an rui_location field.
    >>> doc = {'entity_type': 'Dataset', 'ancestors': [{'rui_location': 'https://example.com'}, {'rui_location': 'https://example2.com'}]}
    >>> _add_spatial_info(doc); doc['is_spatial']; doc['rui_location']
    True
    'https://example2.com'
    '''
    if (doc['entity_type'] == 'Sample'):
        doc['is_spatial'] = doc.get('rui_location', None) is not None
    if (doc['entity_type'] == 'Dataset'):
        ancestors = doc.get('ancestors', [])
        # Find the nearest ancestor with an rui_location - the last one in the list with an rui_location field.
        nearest_rui_location_ancestor = next(
            (ancestor for ancestor in reversed(ancestors) if 'rui_location' in ancestor), None)
        if nearest_rui_location_ancestor is not None:
            doc['is_spatial'] = nearest_rui_location_ancestor is not None
            doc['rui_location'] = nearest_rui_location_ancestor['rui_location']
