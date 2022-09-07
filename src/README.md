# Index Neo4j to ElasticSearch

## About `mapper_metadata.VERSION`

The file named `mapper_metadata.VERSION` in this current directory is used to keep tracking the the version of the indexed data entries in Elasticsearch. The portal-ui also queries this version number from Elasticsearch and shows it at `https://portal.hubmapconsortium.org/dev-search`. Ensuring the version number consitency between the deployed search-api code and the one shows up in portal-ui is critical for data integrity purposes. Before the indexer code reindexes the data from Neo4j, we should increment this version number to indicte this reindexing. 

## Run the indexer as script

When running this indexer as a Python script, it will delete all the existing indices and recreate them then index everything. And it requires to have all the dependencies installed already. Below is the command to run within the search-api container under the source code directory `/usr/src/app/src` (either mounted or copied):

````
python3 -m hubmap_translator <globus-nexus-token>
````

This approach uses the same configuration file `src/instance/app.cfg` so make sure it exists.

By default, the logging output of this script goes to either STDERR or STDOUT. For debugging purpose, we can redirect STDOUT (1) to a file, and then we redirect to STDERR (2) to the new address of 1 (the file). Now both STDOUT and STDERR are going to the same `indexer.log`.

````
python3 -m hubmap_translator <globus-nexus-token> 1>indexer.log 2>&1
````

## Live reindex via HTTP request

The live reindex will NOT recreate the indices, instead it will just delete and documents that are no longer in Neo4j and reindex each entity document found in Neo4j.

````
curl -i -X PUT -H "Authorization:Bearer <globus-nexus-token>" <search-api base URL>/reindex-all
````

The token will need to be in the admin group.

## To debug

Capture one or more documents which fail during indexing. Then, from `src/` run:
```
PYTHONPATH=. hubmap_translation/debug.py ~/failing-doc-1.yaml ~/failing-doc-2.json ...
```
