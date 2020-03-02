import sys
import os
from flask import Flask
from src.elasticsearch.main import Main
import threading
import requests

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper

# For debugging
from pprint import pprint

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

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
    if not request.is_json:
        abort(400, jsonify( { 'error': 'This search request requries a json body' } ))

    json_data = request.get_json()

    user_info = get_user_info_for_access_check(request, True)

    access_group = None

    # If returns error response, invalid header or token
    # Modify the search json and only search on the documents with `access_group` attribute's value as "Open"
    if isinstance(user_info, Response):
        access_group = "Open"
        
    # Otherwise, user_info is a dict and we check user_info['hmgroupids'] list
    # Key 'hmgroupids' presents only when group_required is True
    if app.config['GLOBUS_HUBMAP_READ_GROUP_UUID'] in user_info['hmgroupids']:
        access_group = "Readonly"

    access_group_query = {
      "match_phrase": {
        "access_group": {
          "query": access_group
        }
      }
    }

    # Modify the incoming json data by adding the query
    final_json_data = json_data["query"]["bool"]["must"].append(access_group_query)
    
    # Pass the search json to elasticsearch
    target_url = app.config['ELASTICSEARCH_HOST'] + '/' + '_search'
    response = requests.post(url = target_url, data = final_json_data)

    # return the elasticsearch resulting json
    return response.json()


@app.route('/reindex/<uuid>', methods=['PUT'])
def reindex(uuid):
    try:
        main = Main('entities')
        t1 = threading.Thread(target=main.reindex, args=uuid)
        t1.start()
    except Exception as e:
        print(e)
    return 'OK'


####################################################################################################
## Internal Functions Used By API
####################################################################################################

# Initialize AuthHelper (AuthHelper from HuBMAP commons package)
# HuBMAP commons AuthHelper handles "MAuthorization" or "Authorization"
def init_auth_helper():
    if AuthHelper.isInitialized() == False:
        auth_helper = AuthHelper.create(app.config['GLOBUS_APP_ID'], app.config['GLOBUS_APP_SECRET'])
    else:
        auth_helper = AuthHelper.instance()
    
    return auth_helper


# Get user infomation dict based on the http request(headers)
# `group_required` is a boolean, when True, 'hmgroupids' is in the output
def get_user_info_for_access_check(request, group_required):
    auth_helper = init_auth_helper()
    return auth_helper.getUserInfoUsingRequest(request, group_required)

