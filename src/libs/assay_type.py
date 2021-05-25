from pathlib import Path
from typing import Union, List, TypeVar, Iterable, Dict, Any
import logging
from yaml import safe_load, dump
from yaml.error import YAMLError

# HuBMAP commons
from hubmap_commons.schema_tools import (assert_json_matches_schema,
                                         set_schema_base_path)

# Set logging level (default is warning)
logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

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
        if safe_name in self.definitions:
            self.name = safe_name
        elif safe_name in self.alt_name_map:
            self.name = self.alt_name_map[safe_name]
        else:
            raise RuntimeError(f'No such assay_type {name},'
                               ' even as alternate name')
        this_def = self.definitions[self.name]
        self.description = this_def['description']
        self.primary = this_def['primary']
        self.vitessce_hints = (this_def['vitessce-hints']
                              if 'vitessce-hints' in this_def
                              else [])
        self.contains_pii = this_def.get('contains-pii', True)  # default to True for safety
        self.vis_only = this_def.get('vis-only', False)  # False is more common

    def to_json(self) -> JSONType:
        """
        Returns a JSON-compatible representation of the assay type
        """
        return {'name': self.name, 'primary': self.primary,
                'description': self.description,
                'vitessce-hints': self.vitessce_hints,
                'contains-pii': self.contains_pii,
                'vis-only': self.vis_only}

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
    cases = [('codex',
              False, None, None, None,
              'should be uppercase'),
             ('CODEX',
              True, True, False, False,
              'this one is valid'),
             ('codex_cytokit',
              True, False, False, False,
              'this one is valid'),
             ('salmon_rnaseq_bulk',
              True, False, False, False,
              'this is an alt-name'),
             ('scRNA-Seq-10x',
              True, True, True, False,
              'this is a valid name containing pii'),
             (['PAS', 'Image Pyramid'],
              True, False, False, True,
              'complex alt-name'),
             (['Image Pyramid', 'PAS'],
              True, False, False, True,
              'complex alt-name'),
             (['IMC', 'foo'],
              False, None, None, None,
              'invalid complex name')
             ]
    for name, valid, is_primary, contains_pii, vis_only, note in cases:
        try:
            assay = AssayType(name)
            print(f'{name} produced {assay.name} {assay.description}')
            print(f'{assay.to_json()}')
        except Exception as e:
            print(f'{name} ({note}) -> exception {e}')

    print(dump({
        'all names': sorted(AssayType.iter_names()),
        'primary names': sorted(AssayType.iter_names(primary=True)),
        'non-primary names': sorted(AssayType.iter_names(primary=False))
    }))

if __name__ == '__main__':
    main()
