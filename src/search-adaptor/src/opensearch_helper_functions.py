import os

from flask import abort, jsonify, Flask
import logging
import requests
from urllib.parse import urlparse

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'),
            instance_relative_config=True)


####################################################################################################
## Internal Functions Used By API
####################################################################################################

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

        # execute_query() returns two values
        resp, status_code = execute_query('_search', None, index, es_url, query)

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

    response = requests.post(url=target_url, json=json_data)

    logger.debug(f"==========response status code: {response.status_code} ==========")

    # Only check the response payload size on a successful call
    # If any errors, no way the Elasticsearch response payload is over 10MB
    if response.status_code == 200:
        # Handling response over 10MB with a more useful message instead of AWS API Gateway's default 500 message
        # Note Content-length header is not always provided, we have to calculate
        check_response_payload_size(response.text)

    # Return the Elasticsearch resulting json data and status code
    return jsonify(response.json()), response.status_code


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


"""
Send back useful error message instead of AWS API Gateway's default 500 message
when the response payload size is over 10MB (10485760 bytes)

Parameters
----------
response_text: str
    The http response body string

Returns
-------
flask.Response
    500 response with error message if over the hard limit
"""


def check_response_payload_size(response_text):
    search_result_payload = len(response_text.encode('utf-8'))
    aws_api_gateway_payload_max = 10485760

    if search_result_payload > aws_api_gateway_payload_max:
        msg = f'Search result length {search_result_payload} is larger than allowed maximum of {aws_api_gateway_payload_max} bytes'
        logger.debug(msg)
        internal_server_error(msg)
