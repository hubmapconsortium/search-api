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
  YAML=data/generated/combined-definitions.yaml
  src/consolidate-yaml.py --definitions data/definitions > $YAML

  CMD="src/definitions-yaml-to-schema.py --definitions $YAML --schemas data/generated/"

  echo "Running '$CMD'"
  eval $CMD

  for EXAMPLE in examples/*; do
    TYPE=`basename $EXAMPLE .json`
    src/validate.py --document $EXAMPLE --schema data/generated/$TYPE.schema.yaml
  done
end search-schema/examples
