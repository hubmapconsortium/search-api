# Index Neo4j to ElasticSearch

## About `mapper_metadata.VERSION`

The file named `mapper_metadata.VERSION` in this current directory is used to keep tracking the the version of the indexed data entries in Elasticsearch. The portal-ui also queries this version number from Elasticsearch and shows it at `https://portal.hubmapconsortium.org/dev-search`. Ensuring the version number consitency between the deployed search-api code and the one shows up in portal-ui is critical for data integrity purposes. Before the indexer code reindexes the data from Neo4j, we should increment this version number to indicte this reindexing. 

## Run the indexer as script

When running this indexer as a Python script, it will delete all the existing indices and recreate them then index everything. And it requires to have all the dependencies installed already. For the DEV/TEST/STAGE/PROD deployment, we can just run the below command within the search-api container under the source code directory (either mounted or copied) `src`:

````
python3 -m elasticsearch.indexer
````

This approach uses the same configuration file `src/instance/app.cfg` so make sure it exists.

## Live reindex via HTTP request

The live reindex will NOT recreate the indices, instead it will just delete and documents that are no longer in Neo4j and reindex each entity document found in Neo4j.

````
curl -i -X PUT -H "Authorization:Bearer <globus-nexus-token>" <search-api base URL>/reindex-all
````

The token will need to be in the admin group.

## To debug

Capture one or more documents which fail during indexing. Then, from `src/` run:
```
PYTHONPATH=. elasticsearch/debug.py ~/failing-doc-1.yaml ~/failing-doc-2.json ...
```
