This directory provides code that cleans and reorganizes HuBMAP documents
before they go to the `portal` Elasticsearch index.

It relies on `search-schema` for definitions of allowed enum values.

(I would be very happy if there were tests and linting across this whole repo, but for now this directory is a kingdom unto itself.
There is `.travis.yml` at the top level, but otherwise this is self-contained.)
