#!/usr/bin/env bash
set -o errexit

./generate-build-version.sh
src/hubmap_translation/addl_index_transformations/portal/test.sh
src/search-schema/test.sh
src/search-adaptor/tree/src/libs/test.sh
