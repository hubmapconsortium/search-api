import logging
from pathlib import Path

import requests
import yaml
from flask import jsonify

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


# Get a list of entity uuids via entity-api for a given entity type:
# Collection, Donor, Source, Sample, Dataset, Submission. Case-insensitive.
def get_uuids_by_entity_type(entity_type, request_headers, entity_api_url):
    entity_type = entity_type.lower()

    # Use different entity-api endpoint for Collection
    if entity_type == 'collection':
        # url = app.config['ENTITY_API_URL'] + "/collections?property=uuid"
        url = entity_api_url + "/collections?property=uuid"
    else:
        # url = app.config['ENTITY_API_URL'] + "/" + entity_type + "/entities?property=uuid"
        url = entity_api_url + "/" + entity_type + "/entities?property=uuid"

    response = requests.get(url, headers=request_headers, verify=False)

    if response.status_code != 200:
        return jsonify(error=str(
            "get_uuids_by_entity_type() failed to make a request to entity-api for entity type: " + entity_type)), 500

    uuids_list = response.json()

    return uuids_list


# Gets a list of actually public and private indice names
def get_all_indice_names(all_indices):
    all_names = {}
    try:
        indices = all_indices['indices'].keys()
        for i in indices:
            index_info = {}
            index_names = []
            public_index = all_indices['indices'][i]['public']
            private_index = all_indices['indices'][i]['private']
            index_names.append(public_index)
            index_names.append(private_index)
            index_info[i] = index_names
            all_names.update(index_info)
    except Exception as e:
        raise e

    return all_names


def get_type_description(type_code, type_yaml_file_name):
    filename = 'search-schema/data/definitions/enums/' + type_yaml_file_name + '.yaml'
    type_yaml_file = Path(
        __file__).absolute().parent.parent / filename

    logger.debug(f"========type_code: {type_code}")

    with open(type_yaml_file) as file:
        definition_dict = yaml.safe_load(file)

        logger.info(f"Definition yaml file {type_yaml_file} loaded successfully")

        if type_code in definition_dict:
            definition_desc = definition_dict[type_code]['description']
        else:
            # Return the error message as description
            msg = f"Missing definition key {type_code} in {type_yaml_file}"

            logger.error(msg)

            # Use triple {{{}}}
            definition_desc = f"{{{type_code}}}"

        logger.debug(f"========definition_desc: {definition_desc}")

        return definition_desc


def remove_specific_key_entry(obj, key_to_remove=None):
    if type(obj) == dict:
        if key_to_remove in obj.keys():
            obj.pop(key_to_remove)

        for key in obj.keys():
            remove_specific_key_entry(obj[key], key_to_remove)
    elif type(obj) == list:
        for e in obj:
            remove_specific_key_entry(e, key_to_remove)


# To be used by the full index to ensure the nexus token
# belongs to HuBMAP-Data-Admin group
def user_belongs_to_data_admin_group(user_group_ids, data_admin_group_uuid):
    for group_id in user_group_ids:
        if group_id == data_admin_group_uuid:
            return True

    # By now, no match
    return False
