from enum import Enum
import MetaEnum

# An enumeration support 'in' operations, containing the OpenSearch aggregate queries
# supported by this class
class AggQueryType(str, Enum, metaclass=MetaEnum.MetaEnum):
    MAX = 'max'  # maximum value, or newest timestamp
    MIN = 'min'  # minimum value, or oldest timestamp
