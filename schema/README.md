# Search API Schema

A JSON Schema for documents from the HuBMAP Search API:
The [HuBMAP Portal](https://github.com/hubmapconsortium/portal-ui) depends on there being a consistent document structure.

The wrapped metadata in these documents come from metadata TSVs submitted along with the data;
Their structure is described by [ingest-validation-tools](https://github.com/hubmapconsortium/ingest-validation-tools/tree/master/docs).

## Getting started

Checkout the repo and then:
```
cd schema
pip install -r requirements.txt
pip install -r requirements-dev.txt
./test.sh
```
