#!/usr/bin/env bash
set -o errexit

curl http://127.0.0.1:9200 --silent | grep cluster_name \
  && echo 'Elasticsearch is up!'

# TODO: Confirm that indexing works as expected.