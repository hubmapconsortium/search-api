import sys
import os
from elasticsearch.indexer import Indexer
from flask import Flask, jsonify, abort, request, make_response, json, Response
import threading
import requests
import logging

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
## API
####################################################################################################

# Both HTTP GET and HTTP POST can be used to execute search with body against ElasticSearch REST API. 
@app.route('/search', methods = ['GET', 'POST'])
def search():
    # Always expect a json body
    if not request.is_json:
        bad_request_error("A JSON body and appropriate Content-Type header are required")

    # Parse incoming json string into json data(python dict object)
    json_data = request.get_json()

    user_info = get_user_info_for_access_check(request, True)

    app.logger.info("======user_info======")
    app.logger.info(user_info)

    # If returns error response, invalid token header or missing token
    if isinstance(user_info, Response):
    	# Notify the client with 401 error message if invalid or missing token
        unauthorized_error("A valid globus token in the HTTP 'Authorization: Bearer <globus-token>' header is required")
    # Otherwise, user_info is a dict and we check user_info['hmgroupids'] list
    # Key 'hmgroupids' presents only when group_required is True
    else:
        if app.config['GLOBUS_HUBMAP_READ_GROUP_UUID'] not in user_info['hmgroupids']:
        	# Return 403 error message if user doesn't belong to the HuBMAP-Read group
            forbidden_error("The globus token used in the 'Authorization' header doesn't have the right group access permission")

    # When the user belongs to the HuBMAP read group,
    # simply pass the search json to elasticsearch
    # The request json may contain "access_group" in this case
    target_url = app.config['ELASTICSEARCH_URL'] + '/' + '_search'
    # Make a request with json data
    # The use of json parameter converts python dict to json string and adds content-type: application/json automatically
    resp = requests.post(url = target_url, json = json_data)

    # return the elasticsearch resulting json data as json string
    return jsonify(resp.json())


@app.route('/reindex/<uuid>', methods=['PUT'])
def reindex(uuid):
    try:
        indexer = Indexer('entities', app.config['ELASTICSEARCH_URL'], app.config['ENTITY_WEBSERVICE_URL'])
        t1 = threading.Thread(target=indexer.reindex, args=[uuid])
        t1.start()
        #indexer.reindex(uuid)
    except Exception as e:
        print(e)
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

