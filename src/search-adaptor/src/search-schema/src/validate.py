#!/usr/bin/env python3

import argparse
import sys
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
    document = load_yaml(args.document.read())
    schema = load_yaml(args.schema.read())
    errors = validate(document, schema)
    if errors:
        print(f'Errors in {args.document.name} with {args.schema.name}:')
        details = [
            f'- {".".join(e.absolute_schema_path)}: {e.message}'
            for e in errors]
        print('\n'.join(details))
        return 1
    print(f'No errors in {args.document.name} with {args.schema.name}.')
    return 0


if __name__ == "__main__":
    exit_status = main()
    sys.exit(exit_status)
