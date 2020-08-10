#!/usr/bin/env bash
set -o errexit

. ../../test-utils.sh

start flake8
  flake8 || die 'Try: autopep8 --in-place --aggressive -r .'
end flake8

start doctests
  find src | grep '\.py$' | xargs python -m doctest
end doctests

start yaml-to-schema
  YAML=data/.definitions.yaml
  src/consolidate-yaml.py --definitions data/definitions > $YAML

  REAL_SCHEMAS=data/schemas/
  TEST_SCHEMAS=data/schemas.test/
  CMD="src/definitions-yaml-to-schema.py --definitions $YAML --schemas"

  WHOLE_CMD="$CMD $TEST_SCHEMAS"
  echo "Running '$WHOLE_CMD'"
  eval $WHOLE_CMD

  diff --ignore-blank-lines $REAL_SCHEMAS $TEST_SCHEMAS \
    || die "To refresh: $CMD $REAL_SCHEMAS"
  rm -rf $TEST_SCHEMAS
end yaml-to-schema

start examples
  for EXAMPLE in examples/*; do
    TYPE=`basename $EXAMPLE .json`
    src/validate.py --document $EXAMPLE --schema data/schemas/$TYPE.schema.yaml
  done
end examples
