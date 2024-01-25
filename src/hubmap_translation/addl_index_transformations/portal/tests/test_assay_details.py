from hubmap_translation.addl_index_transformations.portal.add_assay_details import (
    add_assay_details
)

transformation_resources = {
    'ingest_api_soft_assay_url': 'abc123', 'token': 'def456'}


def mock_raw_soft_assay(uuid, headers):
    class MockResponse():
        def __init__(self):
            self.status_code = 200
            self.text = 'Logger call requires this'

        def json(self):
            return {
                "assaytype": "sciRNAseq",
                "contains-pii": True,
                "description": "sciRNA-seq",
                "dir-schema": "scrnaseq-v0",
                "primary": True,
                "tbl-schema": "scrnaseq-v0",
                "vitessce-hints": []
            }

        def raise_for_status(self):
            pass
    return MockResponse()


def test_raw_dataset_type(mocker):
    mocker.patch('requests.get', side_effect=mock_raw_soft_assay)
    input_raw_doc = {
        'uuid': '421007293469db7b528ce6478c00348d',
        'dataset_type': 'RNAseq',
    }

    expected_raw_output_doc = {
        'assay_display_name': ['sciRNA-seq'],
        'dataset_type': 'RNAseq',
        'mapped_data_types': ['sciRNA-seq'],
        'raw_dataset_type': 'RNAseq',
        'uuid': '421007293469db7b528ce6478c00348d',
        'vitessce-hints': [],
        'visualization': False,
    }
    add_assay_details(input_raw_doc, transformation_resources)
    assert input_raw_doc == expected_raw_output_doc


def mock_processed_soft_assay(uuid, headers):
    class MockResponse():
        def __init__(self):
            self.status_code = 200
            self.text = 'Logger call requires this'

        def json(self):
            return {
                "assaytype": "salmon_rnaseq_sciseq",
                "contains-pii": True,
                "description": "sciRNA-seq [Salmon]",
                "primary": False,
                "vitessce-hints": [
                    "is_sc",
                    "rna"
                ]
            }

        def raise_for_status(self):
            pass
    return MockResponse()


def test_processed_dataset_type(mocker):
    mocker.patch('requests.get', side_effect=mock_processed_soft_assay)
    input_processed_doc = {
        'uuid': '22684b9011fc5aea5cb3f89670a461e8',
        'dataset_type': 'RNAseq [Salmon]',
    }

    output_processed_doc = {
        'assay_display_name': ['sciRNA-seq [Salmon]'],
        'dataset_type': 'RNAseq [Salmon]',
        'mapped_data_types': ['sciRNA-seq [Salmon]'],
        'pipeline': 'Salmon',
        'raw_dataset_type': 'RNAseq',
        'uuid': '22684b9011fc5aea5cb3f89670a461e8',
        'vitessce-hints': [
            "is_sc",
            "rna"
        ],
        'visualization': True,
    }
    add_assay_details(input_processed_doc, transformation_resources)
    assert input_processed_doc == output_processed_doc


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


def test_transform_unknown_assay(mocker):
    mocker.patch('requests.get', side_effect=mock_empty_soft_assay)

    unknown_assay_input_doc = {
        'uuid': '69c70762689b20308bb049ac49653342',
        'dataset_type': 'RNAseq [Salmon]',
        'transformation_errors': []
    }

    unknown_assay_output_doc = {
        'assay_display_name': ['RNAseq [Salmon]'],
        'dataset_type': 'RNAseq [Salmon]',
        'mapped_data_types': ['RNAseq [Salmon]'],
        'pipeline': 'Salmon',
        'raw_dataset_type': 'RNAseq',
        'transformation_errors': ['No soft assay information returned.'],
        'uuid': '69c70762689b20308bb049ac49653342',
        'vitessce-hints': ['unknown-assay'],
        'visualization': False,
    }
    add_assay_details(unknown_assay_input_doc, transformation_resources)
    assert unknown_assay_input_doc == unknown_assay_output_doc
