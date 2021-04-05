# HuBMAP Search API Local Development Instructions

The following instructions are for developers to run the search-api as a standalone Python application with the Flask development server for local development and debugging.

## Generate the BUILD file

In the project root directory:

````
./generate-build-version.sh
````

## Install Dependencies

Create a new Python 3.x virtual environment:

````
python3 -m venv venv-hm-search-api
source venv-hm-search-api/bin/activate
````

Then install the dependencies:

````
export COMMONS_BRANCH=master
pip install -r requirements.txt
````

Note: the above example uses the `master` branch of `commons` from Github. You can also specify to use a released version.

## Install Elasticsearch

Install or configure a hosted Elasticsearch instance.  Currently we're using Elasticsearch 7.4.  [Installation Instructions](https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html)

## Configuration

Create a new file named `app.cfg` under `src/instance` directory based off the `app.cfg.example` with the following details:

````
cd src/instance
cp app.cfg.example app.cfg
````

Edit the four fields at the top `app.cfg`, leave the remaining fields in the configuration file as the defaults specified in `app.cfg.example`:

*ELASTICSEARCH_URL* - URL to your Elasticsearch installation from above.
*APP_CLIENT_ID* - A Globus Application Client ID, Nexus scope required.
*APP_CLIENT_SECRET* - The secret associated with the Globus Application Client ID above.
*ENTITY_API_URL* - URL to the HuBMAP entity-api 

Example values:

````
# ElasticSearch Endpoint
# Works regardless of the trailing slash /
# Point to your local ES instance
ELASTICSEARCH_URL = 'http://localhost:8000'

# URL for talking to Entity API on DEV
# Works regardless of the trailing slash
ENTITY_API_URL = 'https://entity-api.dev.hubmapconsortium.org'

# Globus app client ID and secret
APP_CLIENT_ID = '23-29202309-93929-293924'
APP_CLIENT_SECRET = 'fy92lfumf&92m2093/22mkg'

  . . . .

````

## Start the server

Both methods below will run the search-api web service at `http://localhost:5005`.

#### Directly via Python

````
python3 app.py
````

#### With the Flask Development Server

````
cd src
export FLASK_APP=app.py
export FLASK_ENV=development
flask run -p 5005
````


## Run a full reindex

When running this indexer as a Python script, it will delete all the existing indices (defined in the above `app.cfg`) and recreate them then start indexing everything. Below is the command to run under the source code directory `src`, you must provide a valid Globus Nexus token with HuBMAP write access.  By default, the logging output of this script goes to either STDERR or STDOUT. For debugging purpose, we can redirect STDOUT (1) to a file, and then we redirect to STDERR (2) to the new address of 1 (the file). Now both STDOUT and STDERR are going to the same `indexer.log`.

````
python3 -m elasticsearch.indexer <globus-nexus-token> 1>indexer.log 2>&1
````

## Reindex a single entity

Reindex for a single entity given the uuid of the entity via HTTP request.  The reindex will NOT recreate the indices, instead it will just delete the old document and reindex the updated document along with the other entities in it's provenance chain.

````
curl -i -X PUT -H "Authorization:Bearer <globus-nexus-token>" http://localhost:5005/reindex/<uuid>
````
