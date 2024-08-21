from enum import Enum
import MetaEnum

# An enumeration support 'in' operations, containing the fill strategy to execute.
class FillStrategyType(str, Enum, metaclass=MetaEnum.MetaEnum):
    # Write documents to a new, offline index, but leave it for manual activation in Kibana
    CREATE_FILL = 'create_fill'
    # Write documents to a new, offline index, then make it active
    CREATE_FILL_SWAP = 'create_fill_swap'
    # Clone the active index to a new, offline index, add documents to it, then make it active
    CLONE_ADD_SWAP = 'clone_add_swap'
    # Empty the active index, then write documents to it.
    EMPTY_FILL = 'empty_fill'
