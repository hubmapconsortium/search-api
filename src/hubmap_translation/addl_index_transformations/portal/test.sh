#!/usr/bin/env bash
set -o errexit

. test-utils.sh

cd `dirname $0`

start portal/flake8
flake8 \
  || die "Try: autopep8 --in-place --aggressive -r $PWD"
end portal/flake8

start portal/doctests
cd ../../../..
for F in hubmap_translation/addl_index_transformations/portal/*.py; do
  CMD="python -m doctest -o REPORT_NDIFF $F"
  echo $CMD
  eval $CMD
done
cd -
end portal/doctests

start portal/cli
cd ../../../../..
PYTHONPATH="src:$PYTHONPATH" \
  python src/hubmap_translation/addl_index_transformations/portal/__init__.py \
  src/hubmap_translation/addl_index_transformations/portal/tests/fixtures/input-doc.json \
  | grep '"entity_type": "dataset"'
  # Doctest covers the details: Just want to make sure it runs.
cd -
end portal/cli

start portal/pytest
cd ../../../../..
PYTHONPATH="src:$PYTHONPATH" pytest --verbose --log-cli-level WARN
cd -
end portal/pytest