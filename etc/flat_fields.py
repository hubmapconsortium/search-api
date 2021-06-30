#!/usr/bin/env python3

import argparse
from pathlib import Path
import json
import sys
import re

from flatten_json import flatten


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--json', type=Path, required=True,
        help='Nested JSON from ES that lists the fields in an index')
    parser.add_argument(
        '--only', type=str,
        default='hm_public_portal.mappings._doc.properties.',
        help='Only print lines that start with this')
    parser.add_argument(
        '--exclude', type=str, nargs='+',
        default=['.fields.keyword.type', '.copy_to.0'],
        help='Exclude lines that end with these')
    parser.add_argument(
        '--dot', type=str,
        default='.properties.',
        help='Collapse to a single period')
    parser.add_argument(
        '--strip', type=str,
        default='.type',
        help='Suffix to strip')
    args = parser.parse_args()
    nested = json.load(args.json.open())
    for full_path, v in flatten(nested, '.').items():
        if not full_path.startswith(args.only):
            continue
        if any(full_path.endswith(suffix) for suffix in args.exclude):
            continue
        short_path = re.sub(
            re.escape(args.strip) + '$',
            '',
            full_path
        ).replace(args.only, '', 1).replace(args.dot, '.')
        print(f'{short_path}\t{v}')
    return 0


if __name__ == "__main__":
    sys.exit(main())
