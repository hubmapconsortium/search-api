#!/usr/bin/env bash
set -o errexit

start() { echo travis_fold':'start:$1; echo $1; }
end() { echo travis_fold':'end:$1; }
die() { set +v; echo "$*" 1>&2 ; sleep 1; exit 1; }

cd `dirname $0`

start flake8
flake8 \
  || die "Try: autopep8 --in-place --aggressive -r $PWD"
end flake8

start doctests
cd ../../..
for F in elasticsearch/addl_index_transformations/portal/*.py; do
  python -m doctest $F
done
cd -
end doctests
