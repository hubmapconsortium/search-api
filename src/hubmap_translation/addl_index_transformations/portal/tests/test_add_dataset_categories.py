from hubmap_translation.addl_index_transformations.portal.add_dataset_categories import (
    add_dataset_categories
)


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

    add_dataset_categories(hubmap_processed_input_doc)
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
    add_dataset_categories(lab_processed_input_doc)
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
    add_dataset_categories(raw_input_doc)
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
    add_dataset_categories(component_input_doc)
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
    add_dataset_categories(primary_input_doc)
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

    add_dataset_categories(undefined_creation_action_input_doc)
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

    add_dataset_categories(unknown_creation_action_input_doc)
    assert unknown_creation_action_input_doc == unknown_creation_action_output_doc
