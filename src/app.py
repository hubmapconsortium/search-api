import sys
import os
from elasticsearch.indexer import Indexer
from flask import Flask, jsonify, abort, request, make_response, json, Response
import threading
import requests

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper

# For debugging
from pprint import pprint

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['ELASTICSEARCH_URL'] = app.config['ELASTICSEARCH_URL'].strip('/')

# Error handler for 400 Bad request with custom error message
@app.errorhandler(400)
def resource_not_found(e):
    return jsonify(error=str(e)), 404

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
# Since not all clients support GET with body, POST is used here
@app.route('/search', methods = ['POST'])
def search():
    access_group_open = "Open"

    # Always expect a json body
    if not request.is_json:
        abort(400, description = 'This search request requries a json body')

    # Parse incoming json string into json data(python dict object)
    json_data = request.get_json()

    user_info = get_user_info_for_access_check(request, True)

    pprint("======user_info======")
    pprint(user_info)

    modify_query = False

    # If returns error response, invalid header or token
    # Modify the search json and only search on the documents with `access_group` attribute's value as "Open"
    if isinstance(user_info, Response):
        modify_query = True
    # Otherwise, user_info is a dict and we check user_info['hmgroupids'] list
    # Key 'hmgroupids' presents only when group_required is True
    else:
        if app.config['GLOBUS_HUBMAP_READ_GROUP_UUID'] not in user_info['hmgroupids']:
            modify_query = True

    pprint("======modify_query======")
    pprint(modify_query)
    
    # Modify the orgional json data if invalid authorization token provided 
    # or the user doesn't belong to the HuBMAP read group
    # Otherwise pass through the query as is
    if modify_query:
        query_must_list = json_data["query"]["bool"]["must"]
        
        # If by any chance the request json contains match phrase with `access_group`,
        # we'll response 400 error for security concern
        for obj in query_must_list:
        	if 'match_phrase' in obj and 'access_group' in obj['match_phrase']:
        		abort(400, description = "You can not use 'access_group' in request JSON")

        # dict object
        query_to_add = {
            "match_phrase": {
                "access_group": {
                    "query": access_group_open
                }
            }
        }

        # Modify the request json when "access_group" is not used in the request
        query_must_list.append(query_to_add)

    # When the user belongs to the HuBMAP read group,
    # simply pass the search json to elasticsearch
    # The request json may contain "access_group" in this case
    target_url = app.config['ELASTICSEARCH_URL'] + '/' + '_search'
    # Make a request with json data (adds content-type: application/json automatically)
    resp = requests.post(url = target_url, json = json_data)

    # return the elasticsearch resulting json data as json string
    return jsonify(resp.json())


@app.route('/reindex/<uuid>', methods=['PUT'])
def reindex(uuid):
    try:
        indexer = Indexer('entities', app.config['ELASTICSEARCH_URL'], app.config['ENTITY_WEBSERVICE_URL'])
        t1 = threading.Thread(target=indexer.reindex, args=[uuid])
        t1.start()
        # indexer.reindex(uuid)
    except Exception as e:
        print(e)
    return 'OK', 202


####################################################################################################
## Internal Functions Used By API
####################################################################################################

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

