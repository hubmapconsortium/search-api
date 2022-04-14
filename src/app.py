import threading
from pathlib import Path

from flask import request, Response
# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from urllib3.exceptions import InsecureRequestWarning
from yaml import safe_load

from libs.assay_type import AssayType
# Local modules
from opensearch_helper_functions import *

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Set logging format and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class SearchAPI:
    def __init__(self, config, translator_module):
        # Set self based on passed in config parameters
        for key, value in config.items():
            setattr(self, key, value)

        self.translator_module = translator_module

        # Specify the absolute path of the instance folder and use the config file relative to the instance path
        self.app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__))))

        @self.app.errorhandler(400)
        def __http_bad_request(e):
            return self.http_bad_request(e)

        @self.app.errorhandler(401)
        def __http_unauthorized(e):
            return self.http_unauthorized(e)

        @self.app.errorhandler(403)
        def __http_forbidden(e):
            return self.http_forbidden(e)

        @self.app.errorhandler(500)
        def __http_internal_server_error(e):
            return self.http_internal_server_error(e)

        @self.app.route('/', methods=['GET'])
        def __index():
            return self.index()

        @self.app.route('/assaytype', methods=['GET'])
        def __assaytypes():
            return self.assaytypes()

        @self.app.route('/assaytype/<name>', methods=['GET'])
        @self.app.route('/assayname', methods=['POST'])
        def __assayname(name=None):
            return self.assayname(name)

        @self.app.route('/search', methods=['POST'])
        def __search():
            return self.search()

        @self.app.route('/<index_without_prefix>/search', methods=['POST'])
        def __search_by_index(index_without_prefix):
            return self.search_by_index(index_without_prefix)

        @self.app.route('/count', methods=['GET'])
        def __count():
            return self.count()

        @self.app.route('/<index_without_prefix>/count', methods=['GET'])
        def __count_by_index(index_without_prefix):
            return self.count_by_index(index_without_prefix)

        @self.app.route('/indices', methods=['GET'])
        def __indices():
            return self.indices()

        @self.app.route('/status', methods=['GET'])
        def __status():
            return self.status()

        @self.app.route('/reindex/<uuid>', methods=['PUT'])
        def __reindex(uuid):
            return self.reindex(uuid)

        @self.app.route('/reindex-all', methods=['PUT'])
        def __reindex_all():
            return self.reindex_all()

        @self.app.route('/update/<uuid>', methods=['PUT'])
        def __update(uuid):
            return self.update(uuid)

        @self.app.route('/add/<uuid>', methods=['POST'])
        def __add(uuid):
            return self.add(uuid)

        ####################################################################################################
        ## AuthHelper initialization
        ####################################################################################################

        # Initialize AuthHelper class and ensure singleton
        try:
            if AuthHelper.isInitialized() == False:
                self.auth_helper_instance = AuthHelper.create(self.APP_CLIENT_ID, self.APP_CLIENT_SECRET)

                logger.info("Initialized AuthHelper class successfully :)")
            else:
                self.auth_helper_instance = AuthHelper.instance()
        except Exception:
            msg = "Failed to initialize the AuthHelper class"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    ####################################################################################################
    ## Register error handlers
    ####################################################################################################

    # Error handler for 400 Bad Request with custom error message
    def http_bad_request(self, e):
        return jsonify(error=str(e)), 400

    # Error handler for 401 Unauthorized with custom error message
    def http_unauthorized(self, e):
        return jsonify(error=str(e)), 401

    # Error handler for 403 Forbidden with custom error message
    def http_forbidden(self, e):
        return jsonify(error=str(e)), 403

    # Error handler for 500 Internal Server Error with custom error message
    def http_internal_server_error(self, e):
        return jsonify(error=str(e)), 500

    ####################################################################################################
    ## Default route
    ####################################################################################################

    def index(self):
        return "Hello! This is the Search API service :)"

    ####################################################################################################
    ## Assay type API
    ####################################################################################################

    def assaytypes(self):
        primary = None
        simple = False
        for key, val in request.args.items():
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

    def assayname(self, name=None):
        if name is None:
            self.request_json_required(request)
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
    # BUT AWS API Gateway only supports POST with request body
    # general search uses the DEFAULT_INDEX
    def search(self):
        # Always expect a json body
        self.request_json_required(request)

        logger.info("======search with no index provided======")
        logger.info("default_index: " + self.DEFAULT_INDEX_WITHOUT_PREFIX)

        # Determine the target real index in Elasticsearch to be searched against
        # Use the DEFAULT_INDEX_WITHOUT_PREFIX since /search doesn't take any index
        target_index = self.get_target_index(request, self.DEFAULT_INDEX_WITHOUT_PREFIX)

        # get URL for that index
        es_url = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['elasticsearch']['url'].strip(
            '/')

        # Return the elasticsearch resulting json data as json string
        return execute_query('_search', request, target_index, es_url)

    # Both HTTP GET and HTTP POST can be used to execute search with body against ElasticSearch REST API.
    # BUT AWS API Gateway only supports POST with request body
    # Note: the index in URL is not he real index in Elasticsearch, it's that index without prefix
    def search_by_index(self, index_without_prefix):
        # Always expect a json body
        self.request_json_required(request)

        # Make sure the requested index in URL is valid
        self.validate_index(index_without_prefix)

        logger.info("======requested index_without_prefix======")
        logger.info(index_without_prefix)

        # Determine the target real index in Elasticsearch to be searched against
        target_index = self.get_target_index(request, index_without_prefix)

        # get URL for that index
        es_url = self.INDICES['indices'][index_without_prefix]['elasticsearch']['url'].strip('/')

        # Return the elasticsearch resulting json data as json string
        return execute_query('_search', request, target_index, es_url)

    # HTTP GET can be used to execute search with body against ElasticSearch REST API.
    def count(self):
        # Always expect a json body
        self.request_json_required(request)

        logger.info("======count with no index provided======")

        # Determine the target real index in Elasticsearch to be searched against
        target_index = self.get_target_index(request, self.DEFAULT_INDEX_WITHOUT_PREFIX)

        # get URL for that index
        es_url = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['elasticsearch']['url'].strip('/')

        # Return the elasticsearch resulting json data as json string
        return execute_query('_count', request, target_index, es_url)

    # HTTP GET can be used to execute search with body against ElasticSearch REST API.
    # Note: the index in URL is not he real index in Elasticsearch, it's that index without prefix
    def count_by_index(self, index_without_prefix):
        # Always expect a json body
        self.request_json_required(request)

        # Make sure the requested index in URL is valid
        self.validate_index(index_without_prefix)

        logger.info("======requested index_without_prefix======")
        logger.info(index_without_prefix)

        # Determine the target real index in Elasticsearch to be searched against
        target_index = self.get_target_index(request, index_without_prefix)

        # get URL for that index
        es_url = self.INDICES['indices'][index_without_prefix]['elasticsearch']['url'].strip('/')

        # Return the elasticsearch resulting json data as json string
        return execute_query('_count', request, target_index, es_url)

    # Get a list of indices
    def indices(self):
        # Return the resulting json data as json string
        result = {
            "indices": self.get_filtered_indices()
        }

        return jsonify(result)

    # Get the status of Elasticsearch cluster by calling the health API
    # This shows the connection status and the cluster health status (if connected)
    def status(self):
        response_data = {
            # Use strip() to remove leading and trailing spaces, newlines, and tabs
            'version': ((Path(__file__).absolute().parent.parent / 'VERSION').read_text()).strip(),
            'build': ((Path(__file__).absolute().parent.parent / 'BUILD').read_text()).strip(),
            'elasticsearch_connection': False
        }

        target_url = self.DEFAULT_ELASTICSEARCH_URL + '/_cluster/health'
        resp = requests.get(url=target_url)

        if resp.status_code == 200:
            response_data['elasticsearch_connection'] = True

            # If connected, we also get the cluster health status
            status_dict = resp.json()
            # Add new key
            response_data['elasticsearch_status'] = status_dict['status']

        return jsonify(response_data)

    # This reindex function will also reindex Collection and Upload
    # in addition to the Dataset, Donor, Sample entities
    def reindex(self, uuid):
        # Reindex individual document doesn't require the token to belong
        # to the HuBMAP-Data-Admin group
        # since this is being used by entity-api and ingest-api too
        token = self.get_user_token(request.headers)

        try:
            translator = self.init_translator(token)
            threading.Thread(target=translator.translate, args=[uuid]).start()
            # indexer.reindex(uuid)  # for non-thread

            logger.info(f"Started to reindex uuid: {uuid}")
        except Exception as e:
            logger.exception(e)

            internal_server_error(e)

        return f"Request of reindexing {uuid} accepted", 202

    # Live reindex without first deleting and recreating the indices
    # This just deletes the old document and add the latest document of each entity (if still available)
    def reindex_all(self):
        # The token needs to belong to the HuBMAP-Data-Admin group
        # to be able to trigger a live reindex for all documents
        token = self.get_user_token(request.headers, admin_access_required=True)
        saved_request = request.headers

        logger.debug(saved_request)

        try:
            translator = self.init_translator(token)
            threading.Thread(target=translator.translate_all, args=[]).start()

            logger.info('Started live reindex all')
        except Exception as e:
            logger.exception(e)

            internal_server_error(e)

        return 'Request of live reindex all documents accepted', 202

    def update(self, uuid):
        # Update a specific document with the passed in UUID
        # Takes in a document that will replace the existing one

        # Always expect a json body
        self.request_json_required(request)

        token = self.get_user_token(request.headers)
        document = request.json

        try:
            translator = self.init_translator(token)
            threading.Thread(target=translator.update, args=[uuid, document]).start()

            logger.info(f"Started to update document with uuid: {uuid}")
        except Exception as e:
            logger.exception(e)

            internal_server_error(e)

        return f"Request of updating {uuid} accepted", 202

    def add(self, uuid):
        # Create a specific document with the passed in UUID
        # Takes in a document in the body of the request

        # Always expect a json body
        self.request_json_required(request)

        token = self.get_user_token(request.headers)
        document = request.json

        try:
            translator = self.init_translator(token)
            threading.Thread(target=translator.add, args=[uuid, document]).start()

            logger.info(f"Started to add document with uuid: {uuid}")
        except Exception as e:
            logger.exception(e)

            internal_server_error(e)

        return f"Request of adding {uuid} accepted", 202

    # Get user infomation dict based on the http request(headers)
    # `group_required` is a boolean, when True, 'hmgroupids' is in the output
    def get_user_info_for_access_check(self, request, group_required):
        return self.auth_helper_instance.getUserInfoUsingRequest(request, group_required)

    """
    Parase the token from Authorization header
    
    Parameters
    ----------
    request_headers: request.headers
        The http request headers
    admin_access_required : bool
        If the token is required to belong to the HuBMAP-Data-Admin group, default to False
    
    Returns
    -------
    str
        The token string if valid
    """

    def get_user_token(self, request_headers, admin_access_required=False):
        # Get user token from Authorization header
        # getAuthorizationTokens() also handles MAuthorization header but we are not using that here
        try:
            user_token = self.auth_helper_instance.getAuthorizationTokens(request_headers)
        except Exception:
            msg = "Failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            internal_server_error(msg)

        # The user_token is flask.Response on error
        if isinstance(user_token, Response):
            # The Response.data returns binary string, need to decode
            unauthorized_error(user_token.data.decode())

        if admin_access_required:
            # By now the token is already a valid token
            # But we also need to ensure the user belongs to HuBMAP-Data-Admin group
            # in order to execute the live reindex-all
            # Return a 403 response if the user doesn't belong to HuBMAP-Data-Admin group
            if not self.user_in_hubmap_data_admin_group(request):
                forbidden_error("Access not granted")

        return user_token

    """
    Check if the user with token belongs to the HuBMAP-Data-Admin group
    
    Parameters
    ----------
    request : falsk.request
        The flask http request object that containing the Authorization header
        with a valid Globus nexus token for checking group information
    
    Returns
    -------
    bool
        True if the user belongs to HuBMAP-Data-Admin group, otherwise False
    """

    def user_in_hubmap_data_admin_group(self, request):
        try:
            # The property 'hmgroupids' is ALWASYS in the output with using get_user_info()
            # when the token in request is a nexus_token
            user_info = self.get_user_info(request)
            hubmap_data_admin_group_uuid = self.auth_helper_instance.groupNameToId(self.SECURE_GROUP)[
                'uuid']
        except Exception as e:
            # Log the full stack trace, prepend a line with our message
            logger.exception(e)

            # If the token is not a nexus token, no group information available
            # The commons.hm_auth.AuthCache would return a Response with 500 error message
            # We treat such cases as the user not in the HuBMAP-Data-Admin group
            return False

        return hubmap_data_admin_group_uuid in user_info[self.GROUP_ID]

    """
    Get user infomation dict based on the http request(headers)
    The result will be used by the trigger methods
    
    Parameters
    ----------
    request : Flask request object
        The Flask request passed from the API endpoint 
    
    Returns
    -------
    dict
        A dict containing all the user info
    
        {
            "scope": "urn:globus:auth:scope:nexus.api.globus.org:groups",
            "name": "First Last",
            "iss": "https://auth.globus.org",
            "client_id": "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114",
            "active": True,
            "nbf": 1603761442,
            "token_type": "Bearer",
            "aud": ["nexus.api.globus.org", "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114"],
            "iat": 1603761442,
            "dependent_tokens_cache_id": "af2d5979090a97536619e8fbad1ebd0afa875c880a0d8058cddf510fc288555c",
            "exp": 1603934242,
            "sub": "c0f8907a-ec78-48a7-9c85-7da995b05446",
            "email": "email@pitt.edu",
            "username": "username@pitt.edu",
            "hmscopes": ["urn:globus:auth:scope:nexus.api.globus.org:groups"],
        }
    """

    def get_user_info(self, request):
        # `group_required` is a boolean, when True, 'hmgroupids' is in the output
        user_info = self.auth_helper_instance.getUserInfoUsingRequest(request, True)

        logger.debug("======get_user_info()======")
        logger.debug(user_info)

        # It returns error response when:
        # - invalid header or token
        # - token is valid but not nexus token, can't find group info
        if isinstance(user_info, Response):
            # Bubble up the actual error message from commons
            # The Response.data returns binary string, need to decode
            msg = user_info.get_data().decode()
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise Exception(msg)

        return user_info

    # Always expect a json body
    def request_json_required(self, request):
        if not request.is_json:
            bad_request_error("A JSON body and appropriate Content-Type header are required")

    # We'll need to verify the requested index in URL is valid
    def validate_index(self, index_without_prefix):
        separator = ','
        # indices = get_filtered_indices()
        indices = self.INDICES['indices'].keys()

        if index_without_prefix not in indices:
            bad_request_error(f"Invalid index name. Use one of the following: {separator.join(indices)}")

    # Determine the target real index in Elasticsearch bases on the request header and given index (without prefix)
    # The Authorization header with globus token is optional
    # Case #1: Authorization header is missing, default to use the `hm_public_<index_without_prefix>`.
    # Case #2: Authorization header with valid token, but the member doesn't belong to the HuBMAP-Read group, direct the call to `hm_public_<index_without_prefix>`.
    # Case #3: Authorization header presents but with invalid or expired token, return 401 (if someone is sending a token, they might be expecting more than public stuff).
    # Case #4: Authorization header presents with a valid token that has the group access, direct the call to `hm_consortium_<index_without_prefix>`.
    def get_target_index(self, request, index_without_prefix):
        # Case #1 and #2

        target_index = self.INDICES['indices'][index_without_prefix]['public']

        # Keys in request.headers are case insensitive
        if 'Authorization' in request.headers:
            # user_info is a dict
            user_info = self.get_user_info_for_access_check(request, True)

            logger.info("======user_info======")
            logger.info(user_info)

            # Case #3
            if isinstance(user_info, Response):
                # Notify the client with 401 error message
                unauthorized_error(
                    "The globus token in the HTTP 'Authorization: Bearer <globus-token>' header is either invalid or expired.")
            # Otherwise, we check user_info['hmgroupids'] list
            # Key 'hmgroupids' presents only when group_required is True
            else:
                # Case #4
                if self.GLOBUS_HUBMAP_READ_GROUP_UUID in user_info[self.GROUP_ID]:
                    target_index = self.INDICES['indices'][index_without_prefix]['private']

        return target_index

    # Get a list of entity uuids via entity-api for a given entity type:
    # Collection, Donor, Sample, Dataset, Submission. Case-insensitive.
    def get_uuids_by_entity_type(self, entity_type, token):
        entity_type = entity_type.lower()

        request_headers = self.create_request_headers_for_auth(token)

        # Use different entity-api endpoint for Collection
        if entity_type == 'collection':
            url = self.DEFAULT_ENTITY_API_URL + "/collections?property=uuid"
        else:
            url = self.DEFAULT_ENTITY_API_URL + "/" + entity_type + "/entities?property=uuid"

        response = requests.get(url, headers=request_headers, verify=False)

        if response.status_code != 200:
            internal_server_error(
                "get_uuids_by_entity_type() failed to make a request to entity-api for entity type: " + entity_type)

        uuids_list = response.json()

        return uuids_list

    # Create a dict with HTTP Authorization header with Bearer token
    def create_request_headers_for_auth(self, token):
        auth_header_name = 'Authorization'
        auth_scheme = 'Bearer'

        headers_dict = {
            # Don't forget the space between scheme and the token value
            auth_header_name: auth_scheme + ' ' + token
        }

        return headers_dict

    def init_translator(self, token):
        return self.translator_module.Translator(self.INDICES, self.APP_CLIENT_ID, self.APP_CLIENT_SECRET, token)

    # Get a list of filtered Elasticsearch indices to expose to end users without the prefix
    def get_filtered_indices(self):
        # just get all the defined index keys from the yml file
        indices = self.INDICES['indices'].keys()
        return list(indices)


# For local development/testing
if __name__ == "__main__":
    try:
        app.run(host='0.0.0.0', port="5005")
    except Exception as e:
        print("Error during starting debug server.")
        print(str(e))
        logger.error(e, exc_info=True)
        print("Error during startup check the log file for further information")
