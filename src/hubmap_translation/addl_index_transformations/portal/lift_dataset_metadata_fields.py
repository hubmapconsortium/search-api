from hubmap_translation.addl_index_transformations.portal.utils import (
    _log_transformation_error
)


def _get_analyte_class(doc):
    metadata = doc.get('metadata', {}).get('metadata', {})
    return metadata.get('analyte_class')


def _lift_analyte_class(doc):
    if analyte_class := _get_analyte_class(doc):
        doc['analyte_class'] = analyte_class
        return

    for ancestor_doc in doc['ancestors']:
        if analyte_class := _get_analyte_class(ancestor_doc):
            doc['analyte_class'] = analyte_class
            return

    _log_transformation_error(
        doc, 'Analyte_class not found on dataset or dataset ancestors.')


def lift_dataset_metadata_fields(doc):
    if doc['entity_type'] == 'Dataset':
        _lift_analyte_class(doc)
