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
    access_group_open = "Open"

    # Always expect a json body
    if not request.is_json:
        abort(400, description = 'This search request requries a json body')

    # Parse incoming json string into json data(python dict object)
    json_data = request.get_json()

    # The query being sent in the body must be nested in a query key
    if "query" not in json_data:
    	abort(400, description = 'The query being sent in the json body must be nested in a "query" key')

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
        # Error message if 'access_group' used in the orginal query
        err_msg_denied_access_group_usage = "You can not use 'access_group' in request JSON"
        
        # Error message for unsupported query clauses
        err_msg_unsupported_query_clause = "Sorry, the request JSON contains unsupported search query clause"

        # Leaf query to be added for access open data only
        leaf_query_to_add = {
            "match": {
                "access_group": access_group_open
            }
        }

        # We'll first need to decide if the original query is a leaf query or compound query
        # Case of compound query
        if "bool" in json_data["query"]:
            bool_query_obj = json_data["query"]["bool"]

            # "must": The clause (query) must appear in matching documents and will contribute to the score
            if "must" in bool_query_obj and "filter" not in bool_query_obj:
                query_clause_list = bool_query_obj["must"]

            # "filter": The clause (query) must appear in matching documents. However unlike "must" the score of the query will be ignored
            if "filter" in bool_query_obj and "must" not in bool_query_obj:
                query_clause_list = bool_query_obj["filter"]

            # When both "must" and "filter" clauses present, modify the "must" list
            if "must" in bool_query_obj and "filter" in bool_query_obj:
                query_clause_list = bool_query_obj["must"]

            # When neither "must" nor "filter" clause presents, add an empty "must" list
            if "must" not in bool_query_obj and "filter" not in bool_query_obj:
                bool_query_obj["must"] = []
                query_clause_list = bool_query_obj["must"]

            # If by any chance the request json contains `access_group`,
            # we'll response 400 error for security concern
            for obj in query_clause_list:
                # The `access_group` field contains a single word at this moment, 
                # so we'll cover all possible cases below

                # Case 1: simple "match"
                # Matches if one term is a match, doesn't care about the order of terms
                if 'match' in obj and 'access_group' in obj['match']:
                    bad_request(err_msg_denied_access_group_usage)

                # Case 2: "match_phrase"
                # Matches only if the terms come in the same order
                if 'match_phrase' in obj and 'access_group' in obj['match_phrase']:
                    bad_request(err_msg_denied_access_group_usage)
            
            # When the checks pass("access_group" is not used in the request)
            # we'll modify the orginal query with this simple leaf query(dict object)
            query_clause_list.append(leaf_query_to_add)
        
        # Case of leaf query - match
        elif "match" in json_data["query"]:
            orig_leaf_query_obj = json_data["query"]["match"]
            if 'access_group' in orig_leaf_query_obj:
                bad_request(err_msg_denied_access_group_usage)

            # When check passes, convert the leaf query into a compound query with modification
            convert_leaf_to_compound(json_data, "match", leaf_query_to_add)

        # Case of leaf query - match_phrase
        elif "match_phrase" in json_data["query"]:
            orig_leaf_query_obj = json_data["query"]["match_phrase"]
            if 'access_group' in orig_leaf_query_obj:
                bad_request(err_msg_denied_access_group_usage)

            # When check passes, convert the leaf query into a compound query with modification
            convert_leaf_to_compound(json_data, "match_phrase", leaf_query_to_add)

        # Another case of leaf query - term
        elif "term" in json_data["query"]:
            orig_leaf_query_obj = json_data["query"]["term"]
            if 'access_group' in orig_leaf_query_obj:
                bad_request(err_msg_denied_access_group_usage)

            # When check passes, convert the leaf query into a compound query with modification
            convert_leaf_to_compound(json_data, "term", leaf_query_to_add)

        # Other unsupported queries regardless of leaf or compound
        else:
            bad_request(err_msg_unsupported_query_clause)


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

# Convert the orginal leaf query into a compound query with modification
def convert_leaf_to_compound(json_data, key, leaf_query_to_add):
    # Add new property "bool" -> "must"
    json_data["query"]["bool"] = {}
    json_data["query"]["bool"]["must"] = []

    # Move the original leaf query to the must list
    orig_leaf_query = {
        key: json_data["query"][key]
    }

    json_data["query"]["bool"]["must"].append(orig_leaf_query)
    # Also add the access_group match restriction
    json_data["query"]["bool"]["must"].append(leaf_query_to_add)

    # And delete the leaf query from original query otherwise it's invalid format
    del json_data["query"][key]

    pprint (json_data)