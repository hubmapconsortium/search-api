#!/usr/bin/env bash
set -o errexit

./generate-build-version.sh
src/translator/hubmap_translation/addl_index_transformations/portal/test.sh
src/search-schema/test.sh
src/libs/test.sh
