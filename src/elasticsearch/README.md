# Index Neo4j to ElasticSearch

## About `mapper_metadata.VERSION`

The file named `mapper_metadata.VERSION` in this current directory is used to keep tracking the the version of the indexed data entries in Elasticsearch. The portal-ui also queries this version number from Elasticsearch and shows it at `https://portal.hubmapconsortium.org/dev-search`. Ensuring the version number consitency between the deployed search-api code and the one shows up in portal-ui is critical for data integrity purposes. Before the indexer code reindexes the data from Neo4j, we should increment this version number to indicte this reindexing. 

## Initialize the indexer

You can either pass in the configuration items found in `instance/app.cfg.example` via the Flask app context or initialize separately to run as a script:

````
indexer = Indexer(indices, original_doc_type, portal_doc_type, elasticsearch_url, entity_api_url, app_client_id, app_client_secret)
````

## Live reindex

````
curl -i -X PUT -H "Authorization:Bearer <globus-nexus-token>" <search-api base URL>/reindex-all
````

The token will need to be in the admin group.

## To debug

Capture one or more documents which fail during indexing. Then, from `src/` run:
```
PYTHONPATH=. elasticsearch/debug.py ~/failing-doc-1.yaml ~/failing-doc-2.json ...
```
