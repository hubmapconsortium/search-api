import importlib
import os
import sys
from pathlib import Path

from flask import Flask
from yaml import safe_load

sys.path.append("search-adaptor/src")
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

config['GLOBUS_HUBMAP_READ_GROUP_UUID'] = app.config['GLOBUS_HUBMAP_READ_GROUP_UUID']
config['GLOBUS_HUBMAP_DATA_ADMIN_GROUP_UUID'] = app.config['GLOBUS_HUBMAP_DATA_ADMIN_GROUP_UUID']
config['SECURE_GROUP'] = app.config['SECURE_GROUP']
config['GROUP_ID'] = 'group_membership_ids'

config['APP_CLIENT_ID'] = app.config['APP_CLIENT_ID']
config['APP_CLIENT_SECRET'] = app.config['APP_CLIENT_SECRET']

translator_module = importlib.import_module("hubmap_translator")

sys.path.append("libs")
assay_type_module = importlib.import_module("assay_type", "libs")

# This `app` will be imported by wsgi.py when deployed with uWSGI server
app = search_adaptor_module.SearchAPI(config, translator_module, assay_type_module).app

# For local standalone (non-docker) development/testing
if __name__ == "__main__":
    app.run(host='0.0.0.0', port="5005")
