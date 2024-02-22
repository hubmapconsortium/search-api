import pytest

from hubmap_translation.addl_index_transformations.portal.add_assay_details import (
    add_assay_details,
    _add_dataset_categories
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
        'entity_type': 'Dataset',
        'creation_action': 'Create Dataset Activity',
    }

    expected_raw_output_doc = {
        'assay_display_name': ['sciRNA-seq'],
        'dataset_type': 'RNAseq',
        'mapped_data_types': ['sciRNA-seq'],
        'raw_dataset_type': 'RNAseq',
        'uuid': '421007293469db7b528ce6478c00348d',
        'vitessce-hints': [],
        'visualization': False,
        'entity_type': 'Dataset',
        'assay_modality': 'single',
        'creation_action': 'Create Dataset Activity',
        'processing': 'raw'
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
        'entity_type': 'Dataset',
        'creation_action': 'Central Process'
    }

    output_processed_doc = {
        'assay_display_name': ['sciRNA-seq [Salmon]'],
        'dataset_type': 'RNAseq [Salmon]',
        'entity_type': 'Dataset',
        'mapped_data_types': ['sciRNA-seq [Salmon]'],
        'pipeline': 'Salmon',
        'raw_dataset_type': 'RNAseq',
        'assay_modality': 'single',
        'creation_action': 'Central Process',
        'processing': 'processed',
        'processing_type': 'hubmap',
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
        'transformation_errors': [],
        'entity_type': 'Dataset',
        'creation_action': 'Central Process'
    }

    unknown_assay_output_doc = {
        'assay_display_name': ['RNAseq [Salmon]'],
        'assay_modality': 'single',
        'creation_action': 'Central Process',
        'dataset_type': 'RNAseq [Salmon]',
        'mapped_data_types': ['RNAseq [Salmon]'],
        'pipeline': 'Salmon',
        "processing": "processed",
        "processing_type": "hubmap",
        'raw_dataset_type': 'RNAseq',
        'transformation_errors': ['No soft assay information returned.'],
        'uuid': '69c70762689b20308bb049ac49653342',
        'vitessce-hints': ['unknown-assay'],
        'visualization': False,
        'entity_type': 'Dataset',
    }
    add_assay_details(unknown_assay_input_doc, transformation_resources)
    assert unknown_assay_input_doc == unknown_assay_output_doc


def test_hubmap_processing():
    hubmap_processed_input_doc = {
        'creation_action': 'Central Process',
        'descendants': [],
        'entity_type': 'Dataset',
    }

    hubmap_processed_ouput_doc = {
        'assay_modality': 'single',
        'creation_action': 'Central Process',
        'descendants': [],
        'entity_type': 'Dataset',
        'processing': 'processed',
        'processing_type': 'hubmap',
    }

    _add_dataset_categories(hubmap_processed_input_doc, {})
    assert hubmap_processed_input_doc == hubmap_processed_ouput_doc


def test_lab_processing():
    lab_processed_input_doc = {
        'creation_action': 'Lab Process',
        'descendants': [],
        'entity_type': 'Dataset',
    }

    lab_processed_ouput_doc = {
        'assay_modality': 'single',
        'creation_action': 'Lab Process',
        'descendants': [],
        'entity_type': 'Dataset',
        'processing': 'processed',
        'processing_type': 'lab',
    }
    _add_dataset_categories(lab_processed_input_doc, {})
    assert lab_processed_input_doc == lab_processed_ouput_doc


@pytest.mark.parametrize(
    "creation_action, expected_error",
    [
        pytest.param(
            "Conjure Dataset", ['Unrecognized creation action, Conjure Dataset.'], id="unknown"
        ),
        pytest.param(
            None, ['Creation action undefined.'], id="undefined"
        ),
    ]
)
def test_creation_action(creation_action, expected_error):
    creation_action_input_doc = {
        'entity_type': 'Dataset',
        'transformation_errors': [],
    }

    creation_action_output_doc = {
        'entity_type': 'Dataset',
        'transformation_errors': expected_error,
    }

    if creation_action:
        creation_action_input_doc['creation_action'] = creation_action
        creation_action_output_doc['creation_action'] = creation_action

    _add_dataset_categories(creation_action_input_doc, {})
    assert creation_action_input_doc == creation_action_output_doc


@pytest.mark.parametrize(
    "creation_action,is_multi_assay,expected_category,expected_modality,expected_processing",
    [
        pytest.param(
            "Create Dataset Activity", None, None, "single", "raw", id="primary single assay"
        ),
        pytest.param(
            "Create Dataset Activity", True, "primary", "multiple", "raw", id="primary multiassay"
        ),
        pytest.param(
            "Multi-Assay Split", None, "component", "multiple", "raw", id="component"
        ),
        pytest.param(
            "Central Process", True, "processed", "multiple", "processed", id="processed multiassay"
        ),
        pytest.param(
            "Central Process", None, None, "single", "processed", id="processed single assay"
        ),
    ]
)
def test_assay_modality_fields(creation_action, is_multi_assay, expected_category, expected_modality, expected_processing):
    input_doc = {
        'creation_action': creation_action,
        'entity_type': 'Dataset',
    }

    output_doc = {
        'assay_modality': expected_modality,
        'creation_action': creation_action,
        'entity_type': 'Dataset',
        'processing': expected_processing,
    }

    assay_details = {
        "vitessce-hints": []
    }

    if is_multi_assay:
        assay_details['is-multi-assay'] = is_multi_assay

    if expected_processing == "processed":
        output_doc['processing_type'] = "hubmap"

    if expected_category:
        output_doc['multi_assay_category'] = expected_category

    _add_dataset_categories(input_doc, assay_details)
    assert input_doc == output_doc
