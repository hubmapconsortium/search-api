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


def test_raw():
    raw_input_doc = {
        'entity_type': 'Dataset',
        'creation_action': 'Create Dataset Activity',
        'descendants': []
    }

    raw_ouput_doc = {
        'assay_modality': 'single',
        'creation_action': 'Create Dataset Activity',
        'descendants': [],
        'entity_type': 'Dataset',
        'processing': 'raw',
    }
    _add_dataset_categories(raw_input_doc, {})
    assert raw_input_doc == raw_ouput_doc


def test_component():
    component_input_doc = {
        'creation_action': 'Multi-Assay Split',
        'descendants': [],
        'entity_type': 'Dataset',
    }

    component_output_doc = {
        'assay_modality': 'multiple',
        'creation_action': 'Multi-Assay Split',
        'descendants': [],
        'entity_type': 'Dataset',
        'multi_assay_category': 'component',
        'processing': 'raw',
    }
    _add_dataset_categories(component_input_doc, {})
    assert component_input_doc == component_output_doc


def test_primary():
    primary_input_doc = {
        'creation_action': 'Create Dataset Activity',
        'descendants': [
            {
                'creation_action': 'Multi-Assay Split',
            }
        ],
        'entity_type': 'Dataset',
    }

    primary_output_doc = {
        'assay_modality': 'multiple',
        'creation_action': 'Create Dataset Activity',
        'descendants': [
            {
                'creation_action': 'Multi-Assay Split',
            }
        ],
        'entity_type': 'Dataset',
        'multi_assay_category': 'primary',
        'processing': 'raw',
    }

    assay_details = {
        "assaytype": "visium-no-probes",
        "contains-pii": True,
        "dataset-type": "Visium (no probes)",
        "description": "Visium (No probes)",
        "dir-schema": "visium-no-probes-v2",
        "is-multi-assay": True,
        "must-contain": [
            "Histology",
            "RNAseq"
        ],
        "primary": True,
        "vitessce-hints": []
    }
    _add_dataset_categories(primary_input_doc, assay_details)
    assert primary_input_doc == primary_output_doc


def test_undefined_creation_action():
    undefined_creation_action_input_doc = {
        'entity_type': 'Dataset',
        'transformation_errors': [],
    }

    undefined_creation_action_output_doc = {
        'entity_type': 'Dataset',
        'transformation_errors': ['Creation action undefined.'],
    }

    _add_dataset_categories(undefined_creation_action_input_doc, assay_details={})
    assert undefined_creation_action_input_doc == undefined_creation_action_output_doc


def test_unknown_creation_action():
    unknown_creation_action_input_doc = {
        'creation_action': "Conjure Dataset",
        'entity_type': 'Dataset',
        'transformation_errors': [],
    }

    unknown_creation_action_output_doc = {
        'creation_action': "Conjure Dataset",
        'entity_type': 'Dataset',
        'transformation_errors': ['Unrecognized creation action, Conjure Dataset.'],
    }

    _add_dataset_categories(unknown_creation_action_input_doc, {})
    assert unknown_creation_action_input_doc == unknown_creation_action_output_doc
