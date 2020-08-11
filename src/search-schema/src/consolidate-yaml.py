#!/usr/bin/env python3

import argparse
import os
from pathlib import Path
import yaml
import sys


def _dir_path(string):
    if os.path.isdir(string):
        return string
    else:
        raise Exception(f'"{string}" is not a directory')


def main():
    parser = argparse.ArgumentParser(
        description='Consolidate a directory into a single YAML document.')
    parser.add_argument(
        '--definitions', type=_dir_path,
        required=True,
        help='Definitions directory, containing TSVs')

    args = parser.parse_args()
    path = Path(args.definitions)

    output = {
        'fields': read_fields(path / 'fields.yaml'),
        'enums': read_enums(path / 'enums')
    }

    print(yaml.dump(output))
    return 0


def read_fields(path):
    return yaml.safe_load(path.read_text())


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
    for yaml_path in path.glob('*.yaml'):
        name = yaml_path.stem
        enums[name] = yaml.safe_load(yaml_path.read_text())
    return enums


if __name__ == "__main__":
    exit_status = main()
    sys.exit(exit_status)
