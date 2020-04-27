#!/usr/bin/env python3

import argparse
import os
from csv import DictReader
from pathlib import Path
from yaml import dump as dump_yaml
import sys


def _dir_path(string):
    if os.path.isdir(string):
        return string
    else:
        raise Exception(f'"{string}" is not a directory')


def main():
    parser = argparse.ArgumentParser(
        description='Translate a directory of TSVs into YAML.')
    parser.add_argument(
        '--definitions', type=_dir_path,
        required=True,
        help='Definitions directory, containing TSVs')

    args = parser.parse_args()
    path = Path(args.definitions)

    output = {}
    output['fields'] = read_fields(path / 'fields.tsv')
    output['enums'] = read_enums(path / 'enums')

    print(dump_yaml(output))
    return 0


def read_fields(path):
    fields = {}
    with open(path) as f:
        for row in DictReader(f, dialect='excel-tab'):
            fields[row['ES document attribute']] = {
                'neo4j': row['Neo4j Attribute'],
                'required': to_boolean(row['Required Attribute']),
                'description': row['Description'],
                'entity_types': [
                    type.lower() for type in
                    row['Entity types with attribute'].split(', ')
                ]
            }
    return fields


def to_boolean(s):
    '''
    >>> to_boolean('Yes')
    True

    '''
    map = {
        'Yes': True,
        'No': False
    }
    if s in map:
        return map[s]
    return s


def read_enums(path):
    enums = {}
    for tsv_path in path.glob('*.tsv'):
        name = tsv_path.stem
        enums[name] = {}
        with open(tsv_path) as f:
            for row in DictReader(f, dialect='excel-tab'):
                enums[name][row['Value']] = {
                    # As dict for consistency.
                    'description': row['Description']
                }
    return enums


if __name__ == "__main__":
    exit_status = main()
    sys.exit(exit_status)
