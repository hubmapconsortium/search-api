#!/usr/bin/env bash
set -o errexit

BASE=$(dirname $0)
TARGET_DIR=$1
YAML=$TARGET_DIR/combined-definitions.yaml

$BASE/src/consolidate-yaml.py \
  --definitions $BASE/data/definitions > $YAML
$BASE/src/definitions-yaml-to-schema.py \
  --definitions $YAML \
  --schemas $TARGET_DIR/
