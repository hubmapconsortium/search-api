#!/usr/bin/env bash
set -o errexit

./generate-build-version.sh
src/hubmap_translation/addl_index_transformations/portal/test.sh

