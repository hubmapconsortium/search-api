#!/usr/bin/env python3

import sys
from pathlib import Path

from yaml import dump as dump_yaml, safe_load as load_yaml

from elasticsearch.addl_index_transformations.portal import transform


if __name__ == "__main__":
    paths = sys.argv[1:]
    if len(paths) == 0:
        raise Exception('Provide paths to JSON or YAML files as arguments')
    for path in paths:
        doc = load_yaml(Path(path).read_text())
        transformed = transform(doc)
        print(dump_yaml(transformed))
