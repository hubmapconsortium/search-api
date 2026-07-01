# Index Neo4j to ElasticSearch

## About `mapper_metadata.VERSION`

The file named `mapper_metadata.VERSION` in this current directory is used to keep tracking the the version of the indexed data entries in Elasticsearch. The portal-ui also queries this version number from Elasticsearch and shows it at `https://portal.hubmapconsortium.org/dev-search`. Ensuring the version number consitency between the deployed search-api code and the one shows up in portal-ui is critical for data integrity purposes. Before the indexer code reindexes the data from Neo4j, we should increment this version number to indicte this reindexing. 

## Run the indexer as script

When running this indexer as a Python script, it will delete all the existing indices and recreate them then index everything. And it requires to have all the dependencies installed already. Below is the command to run within the search-api container under the source code directory `/usr/src/app/src` (either mounted or copied):

````
python3 -m hubmap_translator <globus-groups-token>
````

This approach uses the same configuration file `src/instance/app.cfg` so make sure it exists.

By default, the logging output of this script goes to either STDERR or STDOUT. For debugging purpose, we can redirect STDOUT (1) to a file, and then we redirect to STDERR (2) to the new address of 1 (the file). Now both STDOUT and STDERR are going to the same `indexer.log`.

````
python3 -m hubmap_translator <globus-groups-token> 1>indexer.log 2>&1
````

## Live reindex via HTTP request

The live reindex will NOT recreate the indices, instead it will just delete and documents that are no longer in Neo4j and reindex each entity document found in Neo4j.

Individual entity reindex requests are handled via a Redis-backed priority queue. When a reindex request is received, the target entity and all of its related entities (ancestors, descendants, revisions, collections, uploads) are enqueued as separate jobs rather than executed immediately. Jobs are processed by worker processes defined in `jobq_workers.py`, which run in the same container as the main service but are started independently of the uWSGI process. Redis must be running and the worker processes must be active for reindex jobs to be executed.

To reindex a single entity:
curl -i -X PUT -H "Authorization:Bearer <globus-groups-token>" <search-api base URL>/reindex/<uuid>
An optional `priority` query parameter controls job priority. Valid values are `1`, `2`, and `3`, where `1` is the highest priority and the default. When a job is enqueued at priority `1`, its related entities are enqueued at priority `2`. Jobs enqueued at priority `2` or `3` have their related entities enqueued at the same priority level.
curl -i -X PUT -H "Authorization:Bearer <globus-groups-token>" <search-api base URL>/reindex/<uuid>?priority=2
To reindex all entities, use the scripts found within `scripts/fresh_indices`. See the `readme.me` in that directory for more details. This replaces the endpoint `/reindex-all`.
## To debug

Capture one or more documents which fail during indexing. Then, from `src/` run:
```
PYTHONPATH=. hubmap_translation/debug.py ~/failing-doc-1.yaml ~/failing-doc-2.json ...
```
