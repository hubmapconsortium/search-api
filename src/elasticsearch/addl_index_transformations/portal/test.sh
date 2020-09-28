#!/usr/bin/env bash
set -o errexit

. test-utils.sh

cd `dirname $0`

start portal/flake8
flake8 \
  || die "Try: autopep8 --in-place --aggressive -r $PWD"
end portal/flake8

start portal/doctests
cd ../../..
for F in elasticsearch/addl_index_transformations/portal/*.py; do
  CMD="python -m doctest -o REPORT_NDIFF $F"
  echo $CMD
  eval $CMD
done
cd -
end portal/doctests
