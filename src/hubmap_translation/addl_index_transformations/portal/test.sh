#!/usr/bin/env bash
set -o errexit


cd `dirname $0`

flake8 
cd ../../../
for F in hubmap_translation/addl_index_transformations/portal/*.py; do
  CMD="python -m doctest -o REPORT_NDIFF $F"
  echo $CMD
  eval $CMD
done
cd -
cd ../../../
PYTHONPATH="src:$PYTHONPATH" pytest -vv  --log-cli-level WARN
cd -
