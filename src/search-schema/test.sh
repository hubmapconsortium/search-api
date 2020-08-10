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

start search-schema/yaml-to-schema
  YAML=data/.definitions.yaml
  src/consolidate-yaml.py --definitions data/definitions > $YAML

  REAL_SCHEMAS=data/generated/
  TEST_SCHEMAS=data/generated.test/
  CMD="src/definitions-yaml-to-schema.py --definitions $YAML --schemas"

  WHOLE_CMD="$CMD $TEST_SCHEMAS"
  echo "Running '$WHOLE_CMD'"
  eval $WHOLE_CMD

  diff --ignore-blank-lines $REAL_SCHEMAS $TEST_SCHEMAS \
    || die "To refresh: $CMD $REAL_SCHEMAS"
  rm -rf $TEST_SCHEMAS
end search-schema/yaml-to-schema

start search-schema/examples
  for EXAMPLE in examples/*; do
    TYPE=`basename $EXAMPLE .json`
    src/validate.py --document $EXAMPLE --schema data/generated/$TYPE.schema.yaml
  done
end search-schema/examples
