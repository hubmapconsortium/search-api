import logging
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml
from flask import jsonify, abort

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


####################################################################################################
# Shared methods for requests
####################################################################################################

# Create a dict with HTTP Authorization header with Bearer token
def create_request_headers_for_auth(token):
    auth_header_name = 'Authorization'
    auth_scheme = 'Bearer'

    headers_dict = {
        # Don't forget the space between scheme and the token value
        auth_header_name: auth_scheme + ' ' + token
    }

    return headers_dict


# Throws error for 400 Bad Reqeust with message
def bad_request_error(err_msg):
    abort(400, description=err_msg)


# Throws error for 401 Unauthorized with message
def unauthorized_error(err_msg):
    abort(401, description=err_msg)


# Throws error for 403 Forbidden with message
def forbidden_error(err_msg):
    abort(403, description=err_msg)


# Throws error for 500 Internal Server Error with message
def internal_server_error(err_msg):
    abort(500, description=err_msg)


def get_uuids_from_es(index, es_url):
    uuids = []
    size = 10_000
    query = {
        "size": size,
        "from": len(uuids),
        "_source": ["_id"],
        "query": {
            "bool": {
                "must": [],
                "filter": [
                    {
                        "match_all": {}
                    }
                ],
                "should": [],
                "must_not": []
            }
        }
    }

    end_of_list = False
    while not end_of_list:
        logger.debug("Searching ES for uuids...")
        logger.debug(es_url)
        resp = execute_query('_search', None, index, es_url, query)
        logger.debug('Got a response from ES...')
        ret_obj = resp.get_json()
        uuids.extend(hit['_id'] for hit in ret_obj.get('hits').get('hits'))

        total = ret_obj.get('hits').get('total').get('value')
        if total <= len(uuids):
            end_of_list = True
        else:
            query['from'] = len(uuids)

    return uuids


# Make a call to Elasticsearch
def execute_query(query_against, request, index, es_url, query=None):
    supported_query_against = ['_search', '_count']
    separator = ','

    if query_against not in supported_query_against:
        bad_request_error(
            f"Query against '{query_against}' is not supported by Search API. Use one of the following: {separator.join(supported_query_against)}")

    # Determine the target real index in Elasticsearch to be searched against
    # index = get_target_index(request, index_without_prefix)

    # target_url = app.config['ELASTICSEARCH_URL'] + '/' + target_index + '/' + query_against
    # es_url = INDICES['indices'][index_without_prefix]['elasticsearch']['url'].strip('/')

    logger.debug('es_url')
    logger.debug(es_url)
    logger.debug(type(es_url))
    # use the index es connection
    target_url = es_url + '/' + index + '/' + query_against

    logger.debug("Target url: " + target_url)
    if query is None:
        # Parse incoming json string into json data(python dict object)
        json_data = request.get_json()

        # All we need to do is to simply pass the search json to elasticsearch
        # The request json may contain "access_group" in this case
        # Will also pass through the query string in URL
        target_url = target_url + get_query_string(request.url)
        # Make a request with json data
        # The use of json parameter converts python dict to json string and adds content-type: application/json automatically
    else:
        json_data = query

    logger.debug(json_data)

    resp = requests.post(url=target_url, json=json_data)
    logger.debug("==========response==========")
    logger.debug(resp)
    try:
        return jsonify(resp.json())
    except Exception as e:
        logger.debug(e)
        raise e
    # Return the elasticsearch resulting json data as json string
    return jsonify(resp)


# Get a list of entity uuids via entity-api for a given entity type:
# Collection, Donor, Source, Sample, Dataset, Submission. Case-insensitive.
def get_uuids_by_entity_type(entity_type, token, entity_api_url):
    entity_type = entity_type.lower()

    request_headers = create_request_headers_for_auth(token)

    # Use different entity-api endpoint for Collection
    if entity_type == 'collection':
        # url = app.config['ENTITY_API_URL'] + "/collections?property=uuid"
        url = entity_api_url + "/collections?property=uuid"
    else:
        # url = app.config['ENTITY_API_URL'] + "/" + entity_type + "/entities?property=uuid"
        url = entity_api_url + "/" + entity_type + "/entities?property=uuid"

    response = requests.get(url, headers=request_headers, verify=False)

    if response.status_code != 200:
        internal_server_error(
            "get_uuids_by_entity_type() failed to make a request to entity-api for entity type: " + entity_type)

    uuids_list = response.json()

    return uuids_list


# Get the query string from orignal request
def get_query_string(url):
    query_string = ''
    parsed_url = urlparse(url)

    logger.debug("======parsed_url======")
    logger.debug(parsed_url)

    # Add the ? at beginning of the query string if not empty
    if not parsed_url.query:
        query_string = '?' + parsed_url.query

    return query_string


####################################################################################################
# Shared methods for translation
####################################################################################################


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
