import os
from pathlib import Path
from elasticsearch.indexer import Indexer
from flask import Flask, jsonify, abort, request, Response, Request
import concurrent.futures
import threading
import requests
import logging
import ast
from urllib.parse import urlparse
from flask import current_app as app

from libs.assay_type import AssayType

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['ELASTICSEARCH_URL'] = app.config['ELASTICSEARCH_URL'].strip('/')

# Set logging level (default is warning)
logging.basicConfig(level=logging.DEBUG)

# Error handler for 400 Bad Request with custom error message
@app.errorhandler(400)
def http_bad_request(e):
    return jsonify(error=str(e)), 400

# Error handler for 401 Unauthorized with custom error message
@app.errorhandler(401)
def http_unauthorized(e):
    return jsonify(error=str(e)), 401

# Error handler for 403 Forbidden with custom error message
@app.errorhandler(403)
def http_forbidden(e):
    return jsonify(error=str(e)), 403

####################################################################################################
## Default route
####################################################################################################

@app.route('/', methods = ['GET'])
def index():
    return "Hello! This is HuBMAP Search API service :)"

####################################################################################################
## Assay type API
####################################################################################################

@app.route('/assaytype', methods = ['GET'])
def assaytypes():
    primary = None
    simple = False
    for key, val in request.args.items():
        print(f'{key}:{val}')
        if key == 'primary':
            primary = val.lower() == "true"
        elif key == 'simple':
            simple = val.lower() == "true"
        else:
            abort(400, f'invalid request parameter {key}')

    if primary is None:
        name_l = [name for name in AssayType.iter_names()]
    else:
        name_l = [name for name in AssayType.iter_names(primary=primary)]
        
    if simple:
        return jsonify(result=name_l)
    else:
        return jsonify(result=[AssayType(name).to_json() for name in name_l])

@app.route('/assaytype/<name>', methods = ['GET'])
@app.route('/assayname', methods = ['POST'])
def assayname(name=None):
    if name is None:
        request_json_required(request)
        try:
            name = request.json['name']
        except Exception:
            abort(400, 'request contains no "name" field')
    try:
        return jsonify(AssayType(name).to_json())
    except Exception as e:
        abort(400, str(e))
        

####################################################################################################
## API
####################################################################################################

# Both HTTP GET and HTTP POST can be used to execute search with body against ElasticSearch REST API. 
@app.route('/search', methods = ['GET', 'POST'])
def search():
    # Always expect a json body
    request_json_required(request)

    app.logger.info("======search with no index provided======")
    
    # Determine the target real index in Elasticsearch to be searched against
    # Use the app.config['DEFAULT_INDEX_WITHOUT_PREFIX'] since /search doesn't take any index
    target_index = get_target_index(request, app.config['DEFAULT_INDEX_WITHOUT_PREFIX'])

    app.logger.info("======target_index======")
    app.logger.info(target_index)

    # Return the elasticsearch resulting json data as json string
    return execute_search(request, target_index)

# Both HTTP GET and HTTP POST can be used to execute search with body against ElasticSearch REST API. 
# Note: the index in URL is not he real index in Elasticsearch, it's that index without prefix
@app.route('/<index_without_prefix>/search', methods = ['GET', 'POST'])
def search_by_index(index_without_prefix):
    # Always expect a json body
    request_json_required(request)

    # Make sure the requested index in URL is valid
    validate_index(index_without_prefix)
    
    app.logger.info("======requested index_without_prefix======")
    app.logger.info(index_without_prefix)

    # Determine the target real index in Elasticsearch to be searched against
    target_index = get_target_index(request, index_without_prefix)

    app.logger.info("======target_index======")
    app.logger.info(target_index)

    # Return the elasticsearch resulting json data as json string
    return execute_search(request, target_index)

# Get a list of indices
@app.route('/indices', methods = ['GET'])
def indices():
    # Return the resulting json data as json string
    result = {
        "indices": get_filtered_indices()
    }

    return jsonify(result)

# Get the status of Elasticsearch cluster by calling the health API
# This shows the connection status and the cluster health status (if connected)
@app.route('/status', methods = ['GET'])
def status():
    response_data = {
        # Use strip() to remove leading and trailing spaces, newlines, and tabs
        'version': (Path(__file__).parent / 'VERSION').read_text().strip(),
        'build': (Path(__file__).parent / 'BUILD').read_text().strip(),
        'elasticsearch_connection': False
    }
    
    target_url = app.config['ELASTICSEARCH_URL'] + '/_cluster/health'
    resp = requests.get(url = target_url)
    
    if resp.status_code == 200:
        response_data['elasticsearch_connection'] = True
        
        # If connected, we also get the cluster health status
        status_dict = resp.json()
        # Add new key
        response_data['elasticsearch_status'] = status_dict['status']

    return jsonify(response_data)

@app.route('/reindex/<uuid>', methods=['PUT'])
def reindex(uuid):
    try:
        indexer = Indexer(app.config['INDICES'],
                          app.config['ELASTICSEARCH_URL'],
                          app.config['ENTITY_WEBSERVICE_URL'])
        threading.Thread(target=indexer.reindex, args=[uuid]).start()
        # indexer.reindex(uuid)
    except Exception as e:
        app.logger.error(e)
    return 'OK', 202


@app.route('/reindex-all', methods=['PUT'])
def reindex_all():
    token = get_nexus_token(request.headers)
    try:
        indexer = Indexer(app.config['INDICES'],
                          app.config['ELASTICSEARCH_URL'],
                          app.config['ENTITY_WEBSERVICE_URL'])
        threading.Thread(target=reindex_all_uuids, args=[indexer, token]).start()
    except Exception as e:
        app.logger.error(e)
    return 'OK', 202

####################################################################################################
## Internal Functions Used By API 
####################################################################################################


# Throws error for 400 Bad Reqeust with message
def bad_request_error(err_msg):
    abort(400, description = err_msg)

# Throws error for 401 Unauthorized with message
def unauthorized_error(err_msg):
    abort(401, description = err_msg)

# Throws error for 403 Forbidden with message
def forbidden_error(err_msg):
    abort(403, description = err_msg)

# Initialize AuthHelper (AuthHelper from HuBMAP commons package)
# HuBMAP commons AuthHelper handles "MAuthorization" or "Authorization"
def init_auth_helper():
    if AuthHelper.isInitialized() == False:
        auth_helper = AuthHelper.create(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'])
    else:
        auth_helper = AuthHelper.instance()
    
    return auth_helper

# Get user infomation dict based on the http request(headers)
# `group_required` is a boolean, when True, 'hmgroupids' is in the output
def get_user_info_for_access_check(request, group_required):
    auth_helper = init_auth_helper()
    return auth_helper.getUserInfoUsingRequest(request, group_required)

def get_nexus_token(request):
    auth_helper = init_auth_helper()
    return auth_helper.getAuthorizationTokens(request)

# Always expect a json body
def request_json_required(request):
    if not request.is_json:
        bad_request_error("A JSON body and appropriate Content-Type header are required")

# We'll need to verify the requested index in URL is valid
def validate_index(index_without_prefix):
    indices = get_filtered_indices()
    if index_without_prefix not in indices:
        bad_request_error("Invalid index name. Use one of the following: " + ', '.join(indices))

# Determine the target real index in Elasticsearch bases on the request header and given index (without prefix)
# The Authorization header with globus token is optional
# Case #1: Authorization header is missing, default to use the `hm_public_<index_without_prefix>`. 
# Case #2: Authorization header with valid token, but the member doesn't belong to the HuBMAP-Read group, direct the call to `hm_public_<index_without_prefix>`. 
# Case #3: Authorization header presents but with invalid or expired token, return 401 (if someone is sending a token, they might be expecting more than public stuff).
# Case #4: Authorization header presents with a valid token that has the group access, direct the call to `hm_consortium_<index_without_prefix>`. 
def get_target_index(request, index_without_prefix):
    # Case #1 and #2
    target_index = app.config['PUBLIC_INDEX_PREFIX'] + index_without_prefix

    # Keys in request.headers are case insensitive 
    if 'Authorization' in request.headers:
        # user_info is a dict
        user_info = get_user_info_for_access_check(request, True)

        app.logger.info("======user_info======")
        app.logger.info(user_info)

        # Case #3
        if isinstance(user_info, Response):
            # Notify the client with 401 error message
            unauthorized_error("The globus token in the HTTP 'Authorization: Bearer <globus-token>' header is either invalid or expired.")
        # Otherwise, we check user_info['hmgroupids'] list
        # Key 'hmgroupids' presents only when group_required is True
        else:
            # Case #4
            if app.config['GLOBUS_HUBMAP_READ_GROUP_UUID'] in user_info['hmgroupids']:
                target_index = app.config['PRIVATE_INDEX_PREFIX'] + index_without_prefix
    
    return target_index

# Make a call to Elasticsearch
def execute_search(request, target_index, query=None):
    if query is None:
        # Parse incoming json string into json data(python dict object)
        json_data = request.get_json()

        # All we need to do is to simply pass the search json to elasticsearch
        # The request json may contain "access_group" in this case
        # Will also pass through the query string in URL
        target_url = app.config['ELASTICSEARCH_URL'] + '/' + target_index + '/' + '_search' + get_query_string(request.url)
        # Make a request with json data
        # The use of json parameter converts python dict to json string and adds content-type: application/json automatically
    else:
        json_data = query
        target_url = (app.config['ELASTICSEARCH_URL'] +
                      '/' + target_index +
                      '/' + '_search')

    resp = requests.post(url=target_url, json=json_data)

    # Return the elasticsearch resulting json data as json string
    return jsonify(resp.json())

# Get a list of filtered Elasticsearch indices to expose to end users without the prefix
def get_filtered_indices():
    # The final list of indices to return
    indices = []

    public_indices_without_prefix = []
    private_indices_without_prefix = []

    # Get a list of all indices and their aliases
    target_url = app.config['ELASTICSEARCH_URL'] + '/_aliases'
    resp = requests.get(url = target_url)
    
    # The JSON that contains all indices and aliases
    indices_and_aliases_dict = resp.json()
    
    # Filter the final list
    # Only return the indices based on prefix naming convention
    for key in indices_and_aliases_dict:
        if key.startswith(app.config['PUBLIC_INDEX_PREFIX']):
            index_without_prefix = key[len(app.config['PUBLIC_INDEX_PREFIX']):]
            public_indices_without_prefix.append(index_without_prefix)

        if key.startswith(app.config['PRIVATE_INDEX_PREFIX']):
            index_without_prefix = key[len(app.config['PRIVATE_INDEX_PREFIX']):]
            private_indices_without_prefix.append(index_without_prefix)

    # Ensure the index pair
    # Basically get intersection of two lists
    indices = list(set(public_indices_without_prefix) & set(private_indices_without_prefix))

    return indices

# Get the query string from orignal request
def get_query_string(url):
    query_string = ''
    parsed_url = urlparse(url)
                
    app.logger.debug("======parsed_url======")
    app.logger.debug(parsed_url)

    # Add the ? at beginning of the query string if not empty
    if not parsed_url.query:
        query_string = '?' + parsed_url.query

    return query_string


def get_uuids_from_neo4j():
    donors = requests.get(app.config['ENTITY_WEBSERVICE_URL'] +
                          "/entities/types/Donor").json()
    samples = requests.get(app.config['ENTITY_WEBSERVICE_URL'] +
                           "/entities/types/Sample").json()
    datasets = requests.get(app.config['ENTITY_WEBSERVICE_URL'] +
                            "/entities/types/Dataset").json()
    collections = requests.get(app.config['ENTITY_WEBSERVICE_URL'] +
                               "/entities/types/Collection").json()

    return (donors['uuids'] + samples['uuids'] + datasets['uuids'] +
            collections['uuids'])


def get_donor_uuids_from_neo4j():
    donors = requests.get(app.config['ENTITY_WEBSERVICE_URL'] +
                          "/entities/types/Donor").json()

    return donors['uuids']


def get_uuids_from_es(index):
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
        resp = execute_search(None,
                              index,
                              query)

        ret_obj = resp.get_json()
        uuids.extend(hit['_id'] for hit in ret_obj.get('hits').get('hits'))

        total = ret_obj.get('hits').get('total').get('value')
        if total <= len(uuids):
            end_of_list = True
        else:
            query['from'] = len(uuids)

    return uuids


def reindex_all_uuids(indexer, token):
    with app.app_context():
        try:
            # 1. remove entities that not in neo4j
            neo4j_uuids = get_uuids_from_neo4j()
            neo4j_uuids = set(neo4j_uuids)
            es_uuids = []
            for index in ast.literal_eval(app.config['INDICES']).keys():
                es_uuids.extend(get_uuids_from_es(index))
            es_uuids = set(es_uuids)

            for uuid in es_uuids:
                if uuid not in neo4j_uuids:
                    app.logger.debug(f"""uuid: {uuid} not in neo4j.
                                        Delete it from Elasticserach.""")
                    indexer.delete(uuid)

            donor_uuids = get_donor_uuids_from_neo4j()
            # 2. Multi-thread index entitiies
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.submit(indexer.reindex, uuid) for uuid
                           in donor_uuids]
                for f in concurrent.futures.as_completed(results):
                    app.logger.debug(f.result())

            # 3. index collection
            indexer.index_collections(token)
            app.logger.info("###Reindex Live is Done###")
        except Exception as e:
            app.logger.error(e)
