#!/usr/bin/env bash
set -o errexit

./generate-build-version.sh
src/elasticsearch/addl_index_transformations/portal/test.sh
src/search-schema/test.sh