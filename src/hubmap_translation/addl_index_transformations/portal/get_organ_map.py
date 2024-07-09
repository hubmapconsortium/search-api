import requests
import logging


logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_organ_map(transformation_resources):
    ontology_url = transformation_resources.get('ontology_url')

    try:
        response = requests.get(
            f'{ontology_url}/organs?application_context=HUBMAP')
        response.raise_for_status()
        organ_json = response.json()
        return {o['rui_code']: o for o in organ_json}
    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        raise
