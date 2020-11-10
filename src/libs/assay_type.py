from pathlib import Path
from typing import Union, List, TypeVar, Iterable
import logging
from yaml import safe_load
from yaml.error import YAMLError

# HuBMAP commons
from hubmap_commons.schema_tools import (assert_json_matches_schema,
                                         set_schema_base_path)

# Set logging level (default is warning)
logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)

BoolOrNone = Union[bool, None]

StrOrListStr = TypeVar('StrOrListStr', str, List[str])

DEFINITION_PATH = (Path(__file__).resolve().parent.parent
                   / 'search-schema' / 'data'
                   / 'definitions' / 'enums' / 'assay_types.yaml')

SCHEMA_PATH = (Path(__file__).resolve().parent) / 'assay_type_schema.yml'
SCHEMA_BASE_URI = 'http://schemata.hubmapconsortium.org/'


class AssayType(object):
    """
    A class intended to represent a single assay type, derived or otherwise.
    """
    definitions = None  # lazy load
    alt_name_map = {}   # map from alt assay names to canonical name

    @classmethod
    def _maybe_load_defs(cls) -> None:
        """If the assay type table has not been loaded, do so."""
        if not(cls.definitions):
            try:
                with open(DEFINITION_PATH) as f:
                    cls.definitions = safe_load(f)
                set_schema_base_path(SCHEMA_PATH.parent, SCHEMA_BASE_URI)
                assert_json_matches_schema(cls.definitions, SCHEMA_PATH)
                for k, v in cls.definitions.items():
                    for alt_k in v['alt-names']:
                        safe_alt_k = (tuple(alt_k) if isinstance(alt_k, list)
                                      else alt_k)
                        cls.alt_name_map[safe_alt_k] = k
            except IOError as e:
                LOGGER.error(f'io error {e} reading assay type table')
                raise
            except YAMLError as e:
                LOGGER.error(f'yaml error {e} reading assay type table')
                raise
            except AssertionError:
                LOGGER.error('assay type table did not match its schema')
                cls.definitions = None
                raise

    def __init__(self, name: StrOrListStr):
        """
        name can be either the canonical name of an assay or an alternate name.

        All names are simple strings, but some alt-names are ordered lists of
        simple strings, e.g. ['IMC', 'image_pyramid'].
        """
        self._maybe_load_defs()
        safe_name = name if isinstance(name, str) else tuple(name)
        print(f'testing {name} {safe_name}')
        if safe_name in self.definitions:
            self.name = safe_name
        elif safe_name in self.alt_name_map:
            self.name = self.alt_name_map[safe_name]
        else:
            raise RuntimeError(f'No such assay_type {name},'
                               ' even as alternate name')
        self.description = self.definitions[self.name]['description']
        self.primary = self.definitions[self.name]['primary']

    @classmethod
    def iter_names(cls, primary: BoolOrNone = None) -> Iterable[str]:
        """
        Returns an iterator over valid canonical assay type names

            primary: controls the subset of assay names returned:
                None (defaulted): iterate over all valid canonical names
                True: iterate only primary assay names, that is, those for
                    which a dataset of this type has no parent which is also
                    a dataset.
             False: iterate only over the names of derived assays, that is,
                    those for which a dataset of the given type has at least
                    one parent which is also a dataset.
        """
        cls._maybe_load_defs()
        if primary is None:
            for key in cls.definitions:
                yield key
        else:
            for key in [k for k in cls.definitions
                        if cls.definitions[k]['primary'] == primary]:
                yield key


def main() -> None:
    """
    Some test routines
    """
    for name, note in [('codex', 'this should fail'),
                       ('CODEX', 'this should work'),
                       ('codex_cytokit', 'this is not primary'),
                       ('salmon_rnaseq_bulk', 'this is an alt name'),
                       (['PAS', 'Image Pyramid'],
                        'this is a complex alt name'),
                       (['IMC', 'foo'],
                        'this is an invalid complex alt name')]:
        try:
            assay = AssayType(name)
            print(f'{name} produced {assay.name} {assay.description}')
        except Exception as e:
            print(f'{name} ({note}) -> exception {e}')

    print('all names:')
    print([k for k in AssayType.iter_names()])
    print('primary names:')
    print([k for k in AssayType.iter_names(primary=True)])
    print('non-primary names:')
    print([k for k in AssayType.iter_names(primary=False)])


if __name__ == '__main__':
    main()
