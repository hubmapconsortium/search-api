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
        bad_request("This search request requries a JSON body")

    # Parse incoming json string into json data(python dict object)
    json_data = request.get_json()

    # The query being sent in the body must be nested in a query key
    if "query" not in json_data:
        bad_request("The query being sent in the JSON body must be nested in a 'query' key")

    # Only one key is allowed in the query outer level - query
    if len(json_data['query']) > 1:
        bad_request("The 'query' context in the JSON body should only contain one top-level key")

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

# Throws error for bad reqeust with message
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
    supported_leaf_query_keys = get_supported_query_keys("leaf")
    supported_compound_query_keys = get_supported_query_keys("compound")

    # The query context dict contains only one key
    query_key = list(query_dict)[0]
    pprint("query key: " + query_key)

    # Leaf query
    if query_key in supported_leaf_query_keys:
        convert_leaf_to_compound(query_dict, query_key, leaf_query_dict_to_add)
    # Compound query
    elif query_key in supported_compound_query_keys:
        modify_compound_query(query_dict, query_key, leaf_query_dict_to_add)
    # Other unsupported queries
    # Regardless of leaf (e.g., match_none) or compound (e.g., boosting query, function_score query)
    else:
        bad_request("Sorry, this Search API doesn't support the provided search query: '" + query_key + "'")

# Key: match_all, match, match_phrase, term
def convert_leaf_to_compound(query_dict, key, leaf_query_dict_to_add):
    # First make sure 'access_group' is not used
    validate_access_group_usage(query_dict[key])

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

# Modify the compund query based on specific query key
def modify_compound_query(query_dict, key, leaf_query_dict_to_add):
    if key == 'bool':
        modify_bool_query(query_dict[key], leaf_query_dict_to_add)

    if key == 'dis_max':
        modify_dis_max_query(query_dict[key], leaf_query_dict_to_add)

# Only modify the bool query object (python dict)
def modify_bool_query(bool_dict, leaf_query_dict_to_add):
    # "must": The clause (query) must appear in matching documents and will contribute to the score
    # "filter": The clause (query) must appear in matching documents. However unlike "must" the score of the query will be ignored
    # When "must" presents (regardless of "filter" presents), modify the "must" list
    # Otherwise, mmodify "filter" clause if it presents or create an empty "must" if no "filter"
    # Both bool_dict["must"] and bool_dict["filter"] are list (they can be dict, we use list here)
    if "must" in bool_dict:
        validate_compound_query_clause_list(bool_dict["must"])

        # When the checks pass("access_group" is not used in the request)
        # we'll modify the orginal query with this simple leaf query(dict object)
        bool_dict["must"].append(leaf_query_dict_to_add)
    else:
        # Modify the "filter" clause if presents
        if "filter" in bool_dict:
            validate_compound_query_clause_list(bool_dict["filter"])
            bool_dict["filter"].append(leaf_query_dict_to_add)
        else:
            # When neither "must" nor "filter" clause presents, add an empty "must" list
            # And no validatation needed
            bool_dict["must"] = []
            bool_dict["must"].append(leaf_query_dict_to_add)

# Disjunction max query
def modify_dis_max_query(dis_max_dict, leaf_query_dict_to_add):
    # "queries" key is required, it's a list that contains one or more query clauses
    if "queries" in dis_max_dict:
        validate_compound_query_clause_list(dis_max_dict["queries"])

        # When the checks pass("access_group" is not used in the request)
        # we'll modify the orginal query with this simple leaf query(dict object)
        dis_max_dict["queries"].append(leaf_query_dict_to_add)
    else:
        bad_request("'queries' is required top-level parameter in 'dis_max' query in request JSON body")

def validate_compound_query_clause_list(query_clause_list):
    for item in query_clause_list:
        for query in get_supported_query_keys("leaf"):
            if query in item:
                validate_access_group_usage(item[query])
        
# Possible TO-DO: check against all nested keys?
# If by any chance the request json contains `access_group` key in the top level,
# we'll response 400 error for security concern
# The `access_group` field contains a single word at this moment
def validate_access_group_usage(dict):
    # Error message if 'access_group' used in the orginal query
    if 'access_group' in dict:
        bad_request("You can not use 'access_group' in request JSON body")

# Leaf and compound query keys shared with other function calls
def get_supported_query_keys(key):
    keys_dict = {
        "leaf": ['match_all', 'match', 'match_phrase', 'multi_match', 'term', 'terms', 'range', 'exists', 'ids', 'type', 'prefix'],
        "compound": ['bool', 'dis_max']
    }

    try:
        return keys_dict[key]
    except KeyError:
        print("Unknown key: '" + key + "' was used when calling get_supported_query_keys()")

