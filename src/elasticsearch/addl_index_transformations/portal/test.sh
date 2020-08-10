#!/usr/bin/env bash
set -o errexit

. test-utils.sh

cd `dirname $0`

start flake8
flake8 \
  || die "Try: autopep8 --in-place --aggressive -r $PWD"
end flake8

start doctests
cd ../../..
for F in elasticsearch/addl_index_transformations/portal/*.py; do
  python -m doctest -o REPORT_NDIFF $F
done
cd -
end doctests
