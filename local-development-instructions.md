# HuBMAP Search API Local Development Instructions

The following instructions are for developers to run the search-api as a standalone Python application with the Flask development server for local development and debugging.

## Generate the BUILD file

In the project root directory:

````
./generate-build-version.sh
````

## Install Dependencies

Create a new virtual environment named `hm-search-api` (the name can be dirrerent) under the `src/` directory:

````
python3 -m venv hm-search-api
source hm-search-api/bin/activate
````

Then install the dependencies:

````
export COMMONS_BRANCH=master
pip install -r requirements.txt
````

Note: the above example uses the `master` branch of `commons` from Github. You can also specify to use a released version.


## Configuration

Create a new file named `app.cfg` under `src/instance` directory based off the `app.cfg.example` with the following details:

````
# ElasticSearch Endpoint
# Works regardless of the trailing slash /
# Point to your local ES instance
ELASTICSEARCH_URL = 'http://localhost:8000'

# Naming convention of indices (always in pair) in Elasticsearch, can NOT be empty
PUBLIC_INDEX_PREFIX = 'hm_public_'
PRIVATE_INDEX_PREFIX = 'hm_consortium_'

# Default index (without prefix) name for `/search` compability, can NOT be empty
DEFAULT_INDEX_WITHOUT_PREFIX = 'entities'

# URL for talking to Entity API on DEV
# Works regardless of the trailing slash
ENTITY_API_URL = 'https://entity-api.dev.hubmapconsortium.org'

# Globus app client ID and secret
APP_CLIENT_ID = ''
APP_CLIENT_SECRET = ''

# Globus Hubmap-Read group UUID
GLOBUS_HUBMAP_READ_GROUP_UUID = '5777527e-ec11-11e8-ab41-0af86edb4424'

# Open: Only entities can open to the public
# All: All entities
# original: directly from neo4j
# transformed: transformed by portal transform method
INDICES = """{
            'hm_public_entities': ('public','original'),
            'hm_consortium_entities': ('consortium', 'original'),
            'hm_public_portal': ('public', 'portal'),
            'hm_consortium_portal': ('consortium', 'portal')
            }"""
ORIGINAL_DOC_TYPE = 'original'
PORTAL_DOC_TYPE = 'portal'
````

## Start Flask Development Server

````
cd src
export FLASK_APP=app.py
export FLASK_ENV=development
flask run -p 5005
````

This will run the search-api at `http://localhost:5005`.

Alternatively, you can also 

````
python3 app.py
````

## Run the indexer as script

When running this indexer as a Python script, it will delete all the existing indices (defined in the above `app.cfg`) and recreate them then start indexing everything. Below is the command to run under the source code directory `src`:

````
python3 -m elasticsearch.indexer <globus-nexus-token>
````

By default, the logging output of this script goes to either STDERR or STDOUT. For debugging purpose, we can redirect STDOUT (1) to a file, and then we redirect to STDERR (2) to the new address of 1 (the file). Now both STDOUT and STDERR are going to the same `indexer.log`.

````
python3 -m elasticsearch.indexer <globus-nexus-token> 1>indexer.log 2>&1
````

## Reindex for a given uuid via HTTP request

The reindex will NOT recreate the indices, instead it will just delete the old document and reindex the updated document.

````
curl -i -X PUT -H "Authorization:Bearer <globus-nexus-token>" http://localhost:5005/reindex/<uuid>
````
