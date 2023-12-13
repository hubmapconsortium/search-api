import importlib
import os
import sys
from pathlib import Path
from flask import Flask
from yaml import safe_load

sys.path.append("search-adaptor/src")
import libs.hubmap_endpoints
search_adaptor_module = importlib.import_module("app", "search-adaptor/src")

config = {}
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'),
            instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# load the index configurations and set the default
config['INDICES'] = safe_load((Path(__file__).absolute().parent / 'instance/search-config.yaml').read_text())
config['DEFAULT_INDEX_WITHOUT_PREFIX'] = config['INDICES']['default_index']

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
config['DEFAULT_ELASTICSEARCH_URL'] = config['INDICES']['indices'][config['DEFAULT_INDEX_WITHOUT_PREFIX']]['elasticsearch']['url'].strip('/')
config['DEFAULT_ENTITY_API_URL'] = config['INDICES']['indices'][config['DEFAULT_INDEX_WITHOUT_PREFIX']]['document_source_endpoint'].strip('/')
config['APP_CLIENT_ID'] = app.config['APP_CLIENT_ID']
config['APP_CLIENT_SECRET'] = app.config['APP_CLIENT_SECRET']
config['AWS_ACCESS_KEY_ID'] = app.config['AWS_ACCESS_KEY_ID']
config['AWS_SECRET_ACCESS_KEY'] = app.config['AWS_SECRET_ACCESS_KEY']
config['AWS_S3_BUCKET_NAME'] = app.config['AWS_S3_BUCKET_NAME']
config['AWS_S3_OBJECT_PREFIX'] = app.config['AWS_S3_OBJECT_PREFIX']
config['AWS_OBJECT_URL_EXPIRATION_IN_SECS'] = app.config['AWS_OBJECT_URL_EXPIRATION_IN_SECS']
config['LARGE_RESPONSE_THRESHOLD'] = app.config['LARGE_RESPONSE_THRESHOLD']
config['PARAM_SEARCH_RECOGNIZED_ENTITIES_BY_INDEX'] = app.config['PARAM_SEARCH_RECOGNIZED_ENTITIES_BY_INDEX']

translator_module = importlib.import_module("hubmap_translator")

sys.path.append("libs")

hubmap_blueprint = libs.hubmap_endpoints.hubmap_blueprint

# This `app` will be imported by wsgi.py when deployed with uWSGI server
app = search_adaptor_module.SearchAPI(config, translator_module, hubmap_blueprint).app

# For local standalone (non-docker) development/testing
if __name__ == "__main__":
    app.run(host='0.0.0.0', port="5005")
