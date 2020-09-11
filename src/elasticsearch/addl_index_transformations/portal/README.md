This directory provides code that cleans and reorganizes HuBMAP documents
before they go to the `portal` Elasticsearch index.

It relies on `src/search-schema` for definitions of allowed enum values.

To debug problems during ingest, save the entity document locally, and from the top of the repo run:
```
PYTHONPATH=src src/elasticsearch/addl_index_transformations/portal/__init__.py ~/bad-doc.json
```

(I would be very happy if there were tests and linting across this whole repo, but for now this directory is a kingdom unto itself.
There is `.travis.yml` at the top level, but otherwise this is self-contained.)
