#!/usr/bin/env bash
set -o errexit

touch src/BUILD
src/elasticsearch/addl_index_transformations/portal/test.sh
src/search-schema/test.sh