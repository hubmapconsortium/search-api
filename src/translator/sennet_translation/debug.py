#!/usr/bin/env python3

import sys
from pathlib import Path
import logging

from yaml import dump as dump_yaml, safe_load as load_yaml

from translator.hubmap_translation.addl_index_transformations.portal import transform


if __name__ == "__main__":
    paths = sys.argv[1:]
    if len(paths) == 0:
        print('Provide paths to JSON or YAML files as arguments')
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG)
    for path in paths:
        doc = load_yaml(Path(path).read_text())
        new_name = f'{path}.transformed.yaml'
        Path(new_name).open('w').write(dump_yaml(transform(doc)))
        print(f'Wrote {new_name}')
