from enum import Enum
import logging

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class CreationAction(str, Enum):
    CREATE_DATASET = 'Create Dataset Activity'
    MULTI_ASSAY_SPLIT = 'Multi-Assay Split'
    CENTRAL_PROCESS = 'Central Process'
    LAB_PROCESS = 'Lab Process'
    CREATE_PUBLICATION = 'Create Publication Activity'


processing_type_map = {
    CreationAction.CENTRAL_PROCESS: 'hubmap',
    CreationAction.LAB_PROCESS: 'lab',
}


def _log_transformation_error(doc, msg):
    doc['transformation_errors'].append(msg)
    logger.info(msg)


def _add_dataset_processing_fields(doc):
    if processing_type := processing_type_map.get(doc['creation_action']):
        doc['processing'] = 'processed'
        doc['processing_type'] = processing_type
    else:
        doc['processing'] = 'raw'


def _is_component_dataset(doc):
    if 'creation_action' in doc and doc['creation_action'] == CreationAction.MULTI_ASSAY_SPLIT:
        return True
    return False

# Currently only handles primary and component datasets.
# As multi-assay datasets begin to be processed, we will transition to getting an is_multi_assay bool from soft assay.


def _add_multi_assay_fields(doc):
    if _is_component_dataset(doc):
        doc['assay_modality'] = 'multiple'
        doc['multi_assay_category'] = 'component'
        return

    for descendant_doc in doc['descendants']:
        if _is_component_dataset(descendant_doc):
            doc['assay_modality'] = 'multiple'
            doc['multi_assay_category'] = 'primary'
            return
    doc['assay_modality'] = 'single'


def add_dataset_categories(doc):
    if doc['entity_type'] == 'Dataset':
        creation_action = doc.get('creation_action')
        if not creation_action:
            error_msg = "Creation action undefined."
            _log_transformation_error(doc, error_msg)
            return

        if creation_action not in {enum.value for enum in CreationAction}:
            error_msg = f"Unrecognized creation action, {creation_action}."
            _log_transformation_error(doc, error_msg)
            return

        _add_dataset_processing_fields(doc)
        _add_multi_assay_fields(doc)
