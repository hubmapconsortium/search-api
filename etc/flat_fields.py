#!/usr/bin/env python3

import argparse
from pathlib import Path
import json
import sys

from flatten_json import flatten


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--json',
        type=Path,
        required=True,
        help='Nested JSON from ES that lists the fields in an index')
    args = parser.parse_args()
    nested = json.load(args.json.open())
    for k, v in flatten(nested, '.').items():
        print(f'{k}\t{v}')
    return 0


if __name__ == "__main__":
    sys.exit(main())
