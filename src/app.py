import sys
import os
from elasticsearch.indexer import Indexer
from flask import Flask, jsonify, abort, request, make_response, json, Response
import threading
import requests

import json

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
    return jsonify(error=str(e)), 400

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
        bad_request('This search request requries a json body')

    # Parse incoming json string into json data(python dict object)
    json_data = request.get_json()

    # The query being sent in the body must be nested in a query key
    if "query" not in json_data:
    	bad_request('The query being sent in the json body must be nested in a "query" key')

    user_info = get_user_info_for_access_check(request, True)

    pprint("======user_info======")
    pprint(user_info)

    # Flag to indicate if to modify the JSON body or not
    invalid_token = False

    # If returns error response, invalid header or token
    # Modify the search json and only search on the documents with `access_group` attribute's value as "Open"
    if isinstance(user_info, Response):
        invalid_token = True
    # Otherwise, user_info is a dict and we check user_info['hmgroupids'] list
    # Key 'hmgroupids' presents only when group_required is True
    else:
        if app.config['GLOBUS_HUBMAP_READ_GROUP_UUID'] not in user_info['hmgroupids']:
            invalid_token = True

    pprint("======invalid_token======")
    pprint(invalid_token)
    
    # Modify the orginal "query" object if invalid or no authorization token provided 
    # or the user doesn't belong to the HuBMAP read group
    # Otherwise pass through the query as is
    if invalid_token:
        modify_query(json_data["query"])

    pprint("=======Final json_data (dict)========")
    pprint(json_data)

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

# Throws error for bad reqeust
def bad_request(err_msg):
    abort(400, description = err_msg)

# Modify the elasticsearch query JSON
def modify_query(query_dict):
    access_group_open = "Open"

    # Leaf query (python dict) to be added for access open data only
    leaf_query_dict_to_add = {
        "match": {
            "access_group": access_group_open
        }
    }

    # We'll first need to decide if the original query is a leaf query or compound query
    # Leaf query being checked: match_all, match, match_phrase, term
    # Compound query being checked: bool

    # Case of compound query - bool query
    if "bool" in query_dict:
        modify_bool_query(query_dict["bool"], leaf_query_dict_to_add)
    # Case of compound query - constant score query
    elif "constant_score" in query_dict:
        modify_constant_score_query(query_dict["bool"], leaf_query_dict_to_add)
    # Case of leaf query - match_all
    elif "match_all" in query_dict:
        convert_leaf_to_compound(query_dict, "match_all", leaf_query_dict_to_add)
    # Case of leaf query - match
    elif "match" in query_dict:
        convert_leaf_to_compound(query_dict, "match", leaf_query_dict_to_add)
    # Case of leaf query - match_phrase
    elif "match_phrase" in query_dict:
        convert_leaf_to_compound(query_dict, "match_phrase", leaf_query_dict_to_add)
    # Another case of leaf query - term
    elif "term" in query_dict:
        convert_leaf_to_compound(query_dict, "term", leaf_query_dict_to_add)
    # Other unsupported queries regardless of leaf (e.g., range) or compound (e.g., dis_max)
    else:
        # Error message for unsupported query clauses
        bad_request("Sorry, the request JSON contains unsupported search query clause")

# Key: match_all, match, match_phrase, term
def convert_leaf_to_compound(query_dict, key, leaf_query_dict_to_add):
    # First make sure 'access_group' is not used
    check_access_group_usage(query_dict[key])

    # When check passes, convert the leaf query into a compound query with modification
    # Convert the orginal leaf query into a compound query with modification
    # Add new property "bool" (dict) -> "must" (list)
    query_dict["bool"] = {}
    query_dict["bool"]["must"] = []

    # Copy the original leaf query to the must list
    orig_leaf_query = {
        key: query_dict[key]
    }

    query_dict["bool"]["must"].append(orig_leaf_query)

    # Also add the access_group match restriction
    query_dict["bool"]["must"].append(leaf_query_dict_to_add)

    # And delete the original leaf query otherwise it's invalid format
    del query_dict[key]

# Only modify the bool query object (python dict)
def modify_bool_query(bool_dict, leaf_query_dict_to_add):
    # "must": The clause (query) must appear in matching documents and will contribute to the score
    # "filter": The clause (query) must appear in matching documents. However unlike "must" the score of the query will be ignored
    # When "must" presents (regardless of "filter" presents), modify the "must" list
    # Otherwise, mmodify "filter" clause if it presents or create an empty "must" if no "filter"
    if "must" in bool_dict:
        validate_query_clause_list(bool_dict["must"])

        # When the checks pass("access_group" is not used in the request)
        # we'll modify the orginal query with this simple leaf query(dict object)
        bool_dict["must"].append(leaf_query_dict_to_add)
    else:
        # Modify the "filter" clause if presents
        if "filter" in bool_dict:
            validate_query_clause_list(bool_dict["filter"])
            bool_dict["filter"].append(leaf_query_dict_to_add)
        else:
            # When neither "must" nor "filter" clause presents, add an empty "must" list
            # And no validatation needed
            bool_dict["must"] = []
            bool_dict["must"].append(leaf_query_dict_to_add)

# If by any chance the request json contains `access_group`,
# we'll response 400 error for security concern
def validate_query_clause_list(query_clause_list):
    for item in query_clause_list:
        # The `access_group` field contains a single word at this moment, 
        # so we'll cover all possible cases below

        # Case 1: simple "match"
        # Matches if one term is a match, doesn't care about the order of terms
        if 'match' in item:
            check_access_group_usage(item['match'])

        # Case 2: "match_phrase"
        # Matches only if the terms come in the same order
        if 'match_phrase' in item:
            check_access_group_usage(item['match_phrase'])

# If by any chance the request json contains `access_group`,
# we'll response 400 error for security concern
def check_access_group_usage(dict):
    # Error message if 'access_group' used in the orginal query
    if 'access_group' in dict:
        bad_request("You can not use 'access_group' in request JSON")
