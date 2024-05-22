import requests
import logging
import re
from enum import Enum

from portal_visualization.builder_factory import has_visualization

from hubmap_translation.addl_index_transformations.portal.utils import (
    _log_transformation_error
)

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


def _get_assay_details(doc, transformation_resources):
    soft_assay_url = transformation_resources.get('ingest_api_soft_assay_url')
    token = transformation_resources.get('token')
    uuid = doc.get('uuid')
    dataset_type = doc.get('dataset_type')

    try:
        response = requests.get(
            f'{soft_assay_url}/{uuid}', headers={'Authorization': f'Bearer {token}'})
        response.raise_for_status()
        json = response.json()
        if not json:
            empty_error_msg = 'No soft assay information returned.'
            return {'description': dataset_type, 'vitessce-hints': ['unknown-assay'], 'error': empty_error_msg}
        return json
    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        raise


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


def _add_multi_assay_fields(doc, assay_details):
    if _is_component_dataset(doc):
        doc['assay_modality'] = 'multiple'
        doc['is_component'] = True
        return

    if assay_details.get('is-multi-assay', False):
        doc['assay_modality'] = 'multiple'
        creation_action = doc.get('creation_action', None)
        if creation_action in [CreationAction.CREATE_DATASET, CreationAction.CENTRAL_PROCESS]:
            doc['is_component'] = False
        else:
            error_msg = f"Unexpected creation_action={creation_action}. is_component will not be set."
            _log_transformation_error(doc, error_msg)
        return

    doc['assay_modality'] = 'single'


def _add_dataset_categories(doc, assay_details):
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
        _add_multi_assay_fields(doc, assay_details)


def _get_descendants(doc, transformation_resources):
    descendants_url = transformation_resources.get(
        'ingest_api_descendants_url')
    token = transformation_resources.get('token')
    uuid = doc.get('uuid')

    try:
        response = requests.get(
            f'{descendants_url}/{uuid}', headers={'Authorization': f'Bearer {token}'})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        raise


def add_assay_details(doc, transformation_resources):
    if 'dataset_type' in doc:
        assay_details = _get_assay_details(doc, transformation_resources)

        doc['raw_dataset_type'] = re.sub(
            "\\[(.*?)\\]", '', doc.get('dataset_type', '')).rstrip()

        if pipeline := re.search("(?<=\\[)[^][]*(?=])", doc.get('dataset_type', '')):
            doc['pipeline'] = pipeline.group()
        # Preserve the previous shape of mapped_data_types.
        doc['assay_display_name'] = [assay_details.get('description')]
        # Remove once the portal-ui has transitioned to use assay_display_name.
        doc['mapped_data_types'] = [assay_details.get('description')]
        doc['vitessce-hints'] = assay_details.get('vitessce-hints')

        _add_dataset_categories(doc, assay_details)

        error_msg = assay_details.get('error')
        if error_msg:
            _log_transformation_error(doc, error_msg)

        def get_assay_type_for_viz(doc):
            return assay_details

        # Check if the main entity can be visualized by portal-visualization.
        has_viz = has_visualization(doc, get_assay_type_for_viz)
        if has_viz:
            doc['visualization'] = True
        else:
            # If an entity doesn't have a visualization,
            # check its descendants for a supporting image pyramid.
            parent_uuid = doc.get('uuid')
            descendants = _get_descendants(doc, transformation_resources)

            # Define a function to get the assay details for a descendant
            def get_assay_type_for_viz(descendant):
                return _get_assay_details(descendant, transformation_resources)

            # Filter any unpublished/non-QA descendants
            descendants = [descendant for descendant in descendants if [
                'Published', 'QA'].count(descendant.get('status')) > 0]
            # Sort by the descendant's last modified timestamp, descending
            descendants.sort(
                key=lambda x: x['last_modified_timestamp'],
                reverse=True)
            # If any remaining descendants have visualization data, set the parent's visualization to True
            for descendant in descendants:
                if has_visualization(descendant, get_assay_type_for_viz, parent_uuid):
                    doc['visualization'] = True
                    break
