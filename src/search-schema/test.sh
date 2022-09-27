#!/usr/bin/env bash
set -o errexit

. test-utils.sh

cd `dirname $0`

start search-schema/flake8
  flake8 || die 'Try: autopep8 --in-place --aggressive -r .'
end search-schema/flake8

start search-schema/doctests
  find src | grep '\.py$' | xargs python -m doctest
end search-schema/doctests

start search-schema/examples
  TARGET=/tmp/search-api
  mkdir -p $TARGET
  ./generate-schemas.sh $TARGET
  for EXAMPLE in examples/*; do
    TYPE=`basename $EXAMPLE .json`
    src/validate.py --document $EXAMPLE --schema $TARGET/$TYPE.schema.yaml
  done
end search-schema/examples
