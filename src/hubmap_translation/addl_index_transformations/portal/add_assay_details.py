import requests
import logging

from portal_visualization.builder_factory import has_visualization

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def _get_assay_details(doc, uuid, request_url, headers):
    dataset_type = doc.get('dataset_type')

    try:
        response = requests.get(request_url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        raise
    json = response.json()
    if not json:
        logger.info(f"No soft assay information returned for dataset ${uuid}.")
        return {'description': dataset_type, 'vitessce-hints': []}
    return json


def add_assay_details(doc, transformation_resources):
    if 'dataset_type' in doc:
        uuid = doc.get('uuid')
        soft_assay_url = transformation_resources.get(
            'ingest_api_soft_assay_url')
        token = transformation_resources.get('token')

        request_url, headers = f'{soft_assay_url}/{uuid}', {
            'Authorization': f'Bearer {token}'}

        assay_details = _get_assay_details(
            doc, uuid=uuid, request_url=request_url, headers=headers)

        # Preserve the previous shape of mapped_data_types.
        doc['mapped_data_types'] = [assay_details.get('description')]
        doc['vitessce-hints'] = assay_details.get('vitessce-hints')

        def get_assay_type_for_viz(doc):
            return requests.get(request_url, headers=headers).json()

        doc['visualization'] = has_visualization(
            doc, get_assay_type_for_viz)
