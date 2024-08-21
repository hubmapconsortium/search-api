from enum import Enum
import MetaEnum

# An enumeration support 'in' operations, containing the allowed block types for an index.
# https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-blocks.html
class IndexBlockType(str, Enum, metaclass=MetaEnum.MetaEnum):
    METADATA = 'metadata'  # Disable metadata changes, such as closing the index.
    READ = 'read'  # Disable read operations.
    READ_ONLY = 'read_only'  # Disable write operations and metadata changes.
    WRITE = 'write'  # Disable write operations. However, metadata changes are still allowed.
    NONE = 'none'  # Locally defined, used to remove other block types in this enumeration.
