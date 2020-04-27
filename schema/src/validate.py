#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path
from yaml import safe_load as load_yaml
from jsonschema import Draft7Validator


def validate(document, schema):
    Draft7Validator.check_schema(schema)
    validator = Draft7Validator(schema)
    return list(validator.iter_errors(document))


def main():
    parser = argparse.ArgumentParser(
        description='Validate document against a JSON Schema. '
        'The document and the schema can both be YAML.')
    parser.add_argument(
        '--document', type=argparse.FileType('r'),
        required=True,
        help='File to validate')
    parser.add_argument(
        '--schema', type=argparse.FileType('r'),
        required=True,
        help='JSON Schema')

    args = parser.parse_args()
    document_path = Path(args.document)
    schema_path = Path(args.schema)

    document = load_yaml(document_path.read())
    schema = load_yaml(schema_path.read())
    errors = validate(document, schema)
    print(errors)
    return 1 if errors else 0


if __name__ == "__main__":
    exit_status = main()
    sys.exit(exit_status)
