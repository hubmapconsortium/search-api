from hubmap_translation.addl_index_transformations.portal import (
    transform
)


input_doc = {
    'uuid': '69c70762689b20308bb049ac49653342',
    'entity_type': 'Dataset',
    'status': 'New',
    'group_name': 'EXT - Outside HuBMAP',
    'origin_samples': [{
        'organ': 'LY'
    }],
    'create_timestamp': 1575489509656,
    'ancestor_ids': ['1234', '5678'],
    'ancestors': [{
        'sample_category': 'section',
        'created_by_user_displayname': 'daniel Cotter'
    }],
    'data_access_level': 'consortium',
    'dataset_type': 'RNAseq [Salmon]',
    'descendants': [{'entity_type': 'Sample or Dataset'}],
    'donor': {
        "metadata": {
            "organ_donor_data": [
             {
                 "data_type": "Nominal",
                 "grouping_concept_preferred_term": "Sex",
                 "preferred_term": "Male"
             }
            ]
        }
    },
    'files': [{
        "description": "OME-TIFF pyramid file",
        "edam_term": "EDAM_1.24.format_3727",
        "is_qa_qc": False,
        "rel_path": "ometiff-pyramids/stitched/expressions/reg1_stitched_expressions.ome.tif",
        "size": 123456789,
        "type": "unknown"
    }],
    'metadata': {
        'metadata': {
            '_random_stuff_that_should_not_be_ui': 'No!',
            'collectiontype': 'No!',
            'data_path': 'No!',
            'metadata_path': 'No!',
            'tissue_id': 'No!',
            'donor_id': 'No!',
            'cell_barcode_size': '123',
            'should_be_int': '123',
            'should_be_float': '123.456',
            'keep_this_field': 'Yes!',
            'is_boolean': '1'
        },
        'dag_provenance_list': [],
    },
    'rui_location': '{"ccf_annotations": ["http://purl.obolibrary.org/obo/UBERON_0001157"]}',
}

expected_output_doc = {'anatomy_0': ['body'],
                       'anatomy_1': ['large intestine', 'lymph node'],
                       'anatomy_2': ['transverse colon'],
                       'ancestor_counts': {'entity_type': {}},
                       'ancestor_ids': ['1234', '5678'],
                       'ancestors': [{'created_by_user_displayname': 'Daniel Cotter',
                                      'mapped_sample_category': 'Section',
                                      'sample_category': 'section'}],
                       'assay_display_name': ['scRNA-seq (10x Genomics) [Salmon]'],
                       'create_timestamp': 1575489509656,
                       'data_access_level': 'consortium',
                       'dataset_type': 'RNAseq [Salmon]',
                       'descendant_counts': {'entity_type': {'Sample or Dataset': 1}},
                       'descendants': [{'entity_type': 'Sample or Dataset'}],
                       'donor': {'mapped_metadata': {'sex': ['Male']},
                                 'metadata': {'organ_donor_data': [{'data_type': 'Nominal',
                                                                    'grouping_concept_preferred_term': 'Sex',
                                                                    'preferred_term': 'Male'}]}},
                       'entity_type': 'Dataset',
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
                       'mapped_data_types': ['scRNA-seq (10x Genomics) [Salmon]'],
                       'mapped_external_group_name': 'Outside HuBMAP',
                       'mapped_metadata': {},
                       'mapped_status': 'New',
                       'metadata': {'dag_provenance_list': [],
                                    'metadata': {'cell_barcode_size': '123',
                                                 'is_boolean': 'TRUE',
                                                 'keep_this_field': 'Yes!',
                                                 'should_be_float': 123.456,
                                                 'should_be_int': 123}},
                       'origin_samples': [{'mapped_organ': 'Lymph Node', 'organ': 'LY'}],
                       'origin_samples_unique_mapped_organs': ['Lymph Node'],
                       'rui_location': '{"ccf_annotations": '
                       '["http://purl.obolibrary.org/obo/UBERON_0001157"]}',
                       'status': 'New',
                       'uuid': '69c70762689b20308bb049ac49653342',
                       'vitessce-hints': ['is_sc', 'rna'],
                       'visualization': True,
                       }


def mock_soft_assay(uuid, headers):
    class MockResponse():
        def __init__(self):
            self.status_code = 200
            self.text = 'Logger call requires this'

        def json(self):
            return {'assaytype': 'salmon_rnaseq_10x', 'contains-pii': False, 'description': 'scRNA-seq (10x Genomics) [Salmon]', 'primary': False, 'vitessce-hints': ['is_sc', 'rna']}

        def raise_for_status(self):
            pass
    return MockResponse()


def test_transform(mocker):
    mocker.patch('requests.get', side_effect=mock_soft_assay)
    transformation_resources = {
        'ingest_api_soft_assay_url': 'abc123', 'token': 'def456'}
    output = transform(input_doc, transformation_resources)
    del output['mapper_metadata']
    assert output == expected_output_doc


def mock_empty_soft_assay(uuid, headers):
    class MockResponse():
        def __init__(self):
            self.status_code = 200
            self.text = 'Logger call requires this'

        def json(self):
            return {}

        def raise_for_status(self):
            pass
    return MockResponse()


expected_output_doc_unknown_assay = expected_output_doc | {'mapped_data_types': [
    'RNAseq [Salmon]'], 'assay_display_name': [
    'RNAseq [Salmon]'], 'visualization': False, 'vitessce-hints': ['unknown-assay'],
    'transformation_errors': ['No soft assay information returned.']}


def test_transform_unknown_assay(mocker):
    mocker.patch('requests.get', side_effect=mock_empty_soft_assay)
    transformation_resources = {
        'ingest_api_soft_assay_url': 'abc123', 'token': 'def456'}
    output = transform(input_doc, transformation_resources)
    del output['mapper_metadata']
    assert output == expected_output_doc_unknown_assay
