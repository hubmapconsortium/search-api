from hubmap_translation.addl_index_transformations.portal.lift_dataset_metadata_fields import (
    lift_dataset_metadata_fields
)


def test_dataset_with_analyte_class():
    dataset_with_analyte_class_input_doc = {
        'entity_type': 'Dataset',
        'metadata': {
            'metadata': {
                'analyte_class': 'RNA'
            }
        }
    }

    dataset_with_analyte_class_output_doc = {
        'analyte_class': 'RNA',
        'entity_type': 'Dataset',
        'metadata': {
            'metadata': {
                'analyte_class': 'RNA'
            }
        }
    }

    lift_dataset_metadata_fields(dataset_with_analyte_class_input_doc)
    assert dataset_with_analyte_class_input_doc == dataset_with_analyte_class_output_doc


def test_dataset_with_analyte_class_ancestor():
    dataset_with_analyte_class_ancestor_input_doc = {
        'ancestors': [
            {
                'metadata': {
                    'metadata': {
                        'analyte_class': 'RNA'
                    }
                }
            }
        ],
        'entity_type': 'Dataset',
    }

    dataset_with_analyte_class_ancestor_output_doc = {
        'analyte_class': 'RNA',
        'ancestors': [
            {
                'metadata': {
                    'metadata': {
                        'analyte_class': 'RNA'
                    }
                }
            }
        ],
        'entity_type': 'Dataset',
    }

    lift_dataset_metadata_fields(dataset_with_analyte_class_ancestor_input_doc)
    assert dataset_with_analyte_class_ancestor_input_doc == dataset_with_analyte_class_ancestor_output_doc


def test_dataset_without_analyte_class():
    dataset_without_analyte_class_input_doc = {
        'ancestors': [],
        'entity_type': 'Dataset',
        'transformation_errors': [],
    }

    dataset_without_analyte_class_output_doc = {
        'ancestors': [],
        'entity_type': 'Dataset',
        'transformation_errors': ['Analyte_class not found on dataset or dataset ancestors.']
    }

    lift_dataset_metadata_fields(dataset_without_analyte_class_input_doc)
    assert dataset_without_analyte_class_input_doc == dataset_without_analyte_class_output_doc
