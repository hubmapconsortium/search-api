import requests
import logging

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
    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        raise
    json = response.json()
    if not json:
        return {'description': dataset_type, 'vitessce-hints': []}
    return json


def add_assay_details(doc, transformation_resources):
    if 'dataset_type' in doc:
        assay_details = _get_assay_details(doc, transformation_resources)
        # Preserve the previous shape of mapped_data_types.
        doc['mapped_data_types'] = [assay_details.get('description')]
        doc['vitessce-hints'] = assay_details.get('vitessce-hints')
