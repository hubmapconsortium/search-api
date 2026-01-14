import pytest

from hubmap_translation.addl_index_transformations.portal.add_assay_details import (
    add_assay_details,
    _add_dataset_categories
)

transformation_resources = {
    'ingest_api_soft_assay_url': 'abc123',
    'descendants_url': 'ghi789',
    'token': 'def456'
}


def mock_response(response_to_mock, status_code=200, text='Logger call requires this'):
    class MockResponse():
        def __init__(self):
            self.status_code = status_code
            self.text = text

        def json(self):
            return response_to_mock

        def raise_for_status(self):
            pass
    return MockResponse()


def mock_empty_descendants():
    return mock_response([])


def mock_raw_soft_assay(uuid=None, headers=None):
    return mock_response({
        "assaytype": "sciRNAseq",
        "contains-pii": True,
        "description": "sciRNA-seq",
        "dir-schema": "scrnaseq-v0",
        "primary": True,
        "tbl-schema": "scrnaseq-v0",
        "vitessce-hints": []
    })


def test_raw_dataset_type(mocker):
    mocker.patch('requests.get', side_effect=[
                 mock_raw_soft_assay(),
                 mock_empty_descendants()])
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
        'processing': 'raw',
        'soft_assaytype': 'sciRNAseq'
    }
    add_assay_details(input_raw_doc, transformation_resources)
    assert input_raw_doc == expected_raw_output_doc


def mock_processed_soft_assay(uuid=None, headers=None):
    return mock_response({
        "assaytype": "salmon_rnaseq_sciseq",
        "contains-pii": True,
        "pipeline-shorthand": "Salmon",
        "description": "sciRNA-seq [Salmon]",
        "primary": False,
        "vitessce-hints": [
            "is_sc",
            "rna"
        ]
    })


def test_processed_dataset_type(mocker):
    mocker.patch('requests.get', side_effect=[
                 mock_processed_soft_assay(),
                 mock_empty_descendants()])
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
        'soft_assaytype': 'salmon_rnaseq_sciseq',
        'vitessce-hints': [
            "is_sc",
            "rna"
        ],
        'visualization': True,
    }
    add_assay_details(input_processed_doc, transformation_resources)
    assert input_processed_doc == output_processed_doc


def mock_empty_soft_assay(uuid=None, headers=None):
    return mock_response({})


def test_transform_unknown_assay(mocker):
    mocker.patch('requests.get', side_effect=[
                 mock_empty_soft_assay(),
                 mock_empty_descendants()])

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


def mock_image_pyramid_parent(uuid=None, headers=None):
    return mock_response({
        "assaytype": "PAS",
        "contains-pii": False,
        "dataset-type": "Histology",
        "description": "PAS Stained Microscopy",
        "dir-schema": "stained-v0",
        "primary": True,
        "tbl-schema": "stained-v0",
        "vitessce-hints": []
    })


def mock_image_pyramid_descendants(uuid=None, headers=None):
    return mock_response([
        # Newer descendant which is not published and gets ignored
        {
            "uuid": "8adc3c31ca84ec4b958ed20a7c4f4920",
            "status": "New",
            "last_modified_timestamp": 1234567891,
        },
        # Good descendant
        {
            "uuid": "8adc3c31ca84ec4b958ed20a7c4f4919",
            "status": "Published",
            "last_modified_timestamp": 1234567890,
        },
        # Older descendant which is published but gets ignored due to newer descendant
        {
            "uuid": "8adc3c31ca84ec4b958ed20a7c4f4918",
            "status": "Published",
            "last_modified_timestamp": 1234567889,
        }
    ])


def mock_image_pyramid_support(uuid=None, headers=None):
    return mock_response({
        "assaytype": "image_pyramid",
        "contains-pii": False,
        "description": "Image Pyramid",
        "primary": False,
        "vitessce-hints": [
            "is_image",
            "is_support",
            "pyramid"
        ]
    })


def test_transform_image_pyramid_parent(mocker):
    mocker.patch('requests.get', side_effect=[
        # initial request to has_visualization with parent entity
        mock_image_pyramid_parent(),
        # request to get descendants of parent entity
        mock_image_pyramid_descendants(),
        # request to get assay details of first descendant (uuid ending in 4919)
        mock_image_pyramid_support(),
        # portal-visualization re-requests parent entity details
        # to determine which type of image pyramid it is
        mock_image_pyramid_parent(),
        # request to get assay details of second descendant (uuid ending in 4918)
        mock_image_pyramid_support(),
        # portal-visualization re-requests parent entity details again
        mock_image_pyramid_parent(),
    ])
    image_pyramid_input_doc = {
        'uuid': '69c70762689b20308bb049ac49653342',
        'dataset_type': 'PAS',
        'entity_type': 'Dataset',
        'creation_action': 'Create Dataset Activity'
    }

    image_pyramid_output_doc = {
        'assay_display_name': ['PAS Stained Microscopy'],
        'assay_modality': 'single',
        'creation_action': 'Create Dataset Activity',
        'dataset_type': 'PAS',
        'mapped_data_types': ['PAS Stained Microscopy'],
        "processing": "raw",
        'raw_dataset_type': 'PAS',
        'uuid': '69c70762689b20308bb049ac49653342',
        'vitessce-hints': [],
        'visualization': True,
        "soft_assaytype": "PAS",
        'entity_type': 'Dataset',
    }

    add_assay_details(image_pyramid_input_doc, transformation_resources)
    assert image_pyramid_input_doc == image_pyramid_output_doc


def test_transform_image_pyramid_support(mocker):
    mocker.patch('requests.get', side_effect=[
        mock_image_pyramid_support(),
        mock_empty_descendants(),
    ])
    image_pyramid_input_doc = {
        'uuid': '0bf9cb40adebcfb261dfbe9244607508',
        'dataset_type': 'Histology [Image Pyramid]',
        'entity_type': 'Dataset',
        'creation_action': 'Central Process'
    }

    image_pyramid_output_doc = {
        'assay_display_name': ['Image Pyramid'],
        'assay_modality': 'single',
        'creation_action': 'Central Process',
        'dataset_type': 'Histology [Image Pyramid]',
        'mapped_data_types': ['Image Pyramid'],
        "processing": "processed",
        'raw_dataset_type': 'Histology',
        'uuid': '0bf9cb40adebcfb261dfbe9244607508',
        'pipeline': 'Image Pyramid',
        'processing_type': 'hubmap',
        'vitessce-hints': [
            "is_image",
            "is_support",
            "pyramid",

        ],
        'visualization': False,
        "soft_assaytype": "image_pyramid",
        'entity_type': 'Dataset',
    }

    add_assay_details(image_pyramid_input_doc, transformation_resources)
    assert image_pyramid_input_doc == image_pyramid_output_doc


def mock_epic(uuid=None, headers=None):
    return mock_response({
        "assaytype": None,
        "description": "Segmentation Mask",
        "is-multi-assay": False,
        "pipeline-shorthand": "",
        "primary": False,
        "vitessce-hints": [
            "segmentation_mask",
            "is_image",
            "pyramid"
        ]
    })


def test_transform_epic(mocker):
    mocker.patch('requests.get', side_effect=[
        mock_epic(),
        mock_empty_descendants(),
    ])
    epic_input_doc = {
        'uuid': 'abc123',
        'dataset_type': 'Segmentation Mask',
        'entity_type': 'Dataset',
        'creation_action': 'External Process'
    }

    epic_output_doc = {
        'assay_display_name': ['Segmentation Mask'],
        'assay_modality': 'single',
        'creation_action': 'External Process',
        'dataset_type': 'Segmentation Mask',
        'mapped_data_types': ['Segmentation Mask'],
        "processing": "processed",
        'raw_dataset_type': 'Segmentation Mask',
        'uuid': 'abc123',
        'pipeline': 'Segmentation Mask',
        'processing_type': 'external',
        'vitessce-hints': [
            "segmentation_mask",
            "is_image",
            "pyramid",

        ],
        'visualization': False,
        'entity_type': 'Dataset',
    }

    add_assay_details(epic_input_doc, transformation_resources)
    assert epic_input_doc == epic_output_doc


def test_hubmap_processing():
    hubmap_processed_input_doc = {
        'creation_action': 'Central Process',
        'descendants': [],
        'entity_type': 'Dataset',
    }

    hubmap_processed_output_doc = {
        'assay_modality': 'single',
        'creation_action': 'Central Process',
        'descendants': [],
        'entity_type': 'Dataset',
        'processing': 'processed',
        'processing_type': 'hubmap',
    }

    _add_dataset_categories(hubmap_processed_input_doc, {})
    assert hubmap_processed_input_doc == hubmap_processed_output_doc


def test_lab_processing():
    lab_processed_input_doc = {
        'creation_action': 'Lab Process',
        'descendants': [],
        'entity_type': 'Dataset',
    }

    lab_processed_output_doc = {
        'assay_modality': 'single',
        'creation_action': 'Lab Process',
        'descendants': [],
        'entity_type': 'Dataset',
        'processing': 'processed',
        'processing_type': 'lab',
    }
    _add_dataset_categories(lab_processed_input_doc, {})
    assert lab_processed_input_doc == lab_processed_output_doc


def test_external_processing():
    externally_processed_input_doc = {
        'creation_action': 'External Process',
        'descendants': [],
        'entity_type': 'Dataset',
    }

    externally_processed_output_doc = {
        'assay_modality': 'single',
        'creation_action': 'External Process',
        'descendants': [],
        'entity_type': 'Dataset',
        'processing': 'processed',
        'processing_type': 'external',
    }
    _add_dataset_categories(externally_processed_input_doc, {})
    assert externally_processed_input_doc == externally_processed_output_doc


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
    "creation_action,is_multi_assay,expected_component_bool,expected_modality,expected_processing",
    [
        pytest.param(
            "Create Dataset Activity", None, None, "single", "raw", id="primary single assay"
        ),
        pytest.param(
            "Create Dataset Activity", True, False, "multiple", "raw", id="primary multiassay"
        ),
        pytest.param(
            "Multi-Assay Split", None, True, "multiple", "raw", id="component"
        ),
        pytest.param(
            "Central Process", True, False, "multiple", "processed", id="processed multiassay"
        ),
        pytest.param(
            "Central Process", None, None, "single", "processed", id="processed single assay"
        ),
    ]
)
def test_assay_modality_fields(creation_action, is_multi_assay, expected_component_bool, expected_modality, expected_processing):
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

    if expected_modality == 'multiple':
        output_doc['is_component'] = expected_component_bool

    _add_dataset_categories(input_doc, assay_details)
    assert input_doc == output_doc
