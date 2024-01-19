import requests
import logging
import re

from portal_visualization.builder_factory import has_visualization

from hubmap_translation.addl_index_transformations.portal.utils import (
    _log_transformation_error
)

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


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

        error_msg = assay_details.get('error')
        if error_msg:
            _log_transformation_error(doc, error_msg)

        def get_assay_type_for_viz(doc):
            return assay_details

        doc['visualization'] = has_visualization(
            doc, get_assay_type_for_viz)
