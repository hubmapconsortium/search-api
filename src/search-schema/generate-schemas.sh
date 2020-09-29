#!/usr/bin/env bash
set -o errexit

BASE=$(dirname $0)
YAML=$BASE/data/generated/combined-definitions.yaml
$BASE/src/consolidate-yaml.py \
  --definitions $BASE/data/definitions > $YAML
$BASE/src/definitions-yaml-to-schema.py \
  --definitions $YAML \
  --schemas $BASE/data/generated/
