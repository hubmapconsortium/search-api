#!/usr/bin/env python3

import argparse
from yaml import dump as dump_yaml, safe_load as load_yaml
from pathlib import Path
import sys


def _dir_path(string):
    path = Path(string)
    path.mkdir(parents=True, exist_ok=True)
    return path


def main():
    parser = argparse.ArgumentParser(
        description='Translate definitions as YAML into JSON Schemas.')
    parser.add_argument(
        '--definitions', type=argparse.FileType('r'),
        required=True,
        help='Definitions YAML')
    parser.add_argument(
        '--schemas', type=_dir_path,
        required=True,
        help='Output directory for JSON Schema')

    args = parser.parse_args()
    definitions = load_yaml(args.definitions.read())

    for entity_type in ['donor', 'sample', 'dataset', 'collection']:
        path = args.schemas / f'{entity_type}.schema.yaml'
        path.write_text(dump_yaml(make_schema(entity_type, definitions)))

    return 0


def make_schema(entity_type, definitions, top_level=True):
    properties = {
        k: {
            'description': v['description'],
            **optional_enum(v['enum'], definitions)
        }
        for k, v in definitions['fields'].items()
        if entity_type in v['entity_types']
    }
    required = [
        k
        for k, v in definitions['fields'].items()
        if entity_type in v['entity_types']
        and v['required'] is True
        # TODO: Some (true-y) strings are used for special cases.
    ]
    if top_level:
        for extra in [
                'access_group',
                'ancestor_ids', 'ancestors',
                'descendant_ids', 'descendants']:
            properties[extra] = {'description': 'TODO'}
            required.append(extra)
        if 'donor' in properties:
            properties['donor'] = make_schema(
                'donor', definitions, top_level=False)
        if 'origin_sample' in properties:
            properties['origin_sample'] = make_schema(
                'sample', definitions, top_level=False)
        if 'source_sample' in properties:
            properties['source_sample'] = {
                # TODO: Is this correct?
                # I was expecting just an object.
                'type': 'array',
                'items': make_schema(
                    'sample', definitions, top_level=False)
            }
    schema = {
        'type': 'object',
        'properties': properties,
        'required': required,
        'additionalProperties': False
    }
    return schema


def optional_enum(enum_name, definitions):
    if not enum_name:
        return {}
    return {
        'enum': list(definitions['enums'][enum_name].keys())
    }


if __name__ == "__main__":
    exit_status = main()
    sys.exit(exit_status)
