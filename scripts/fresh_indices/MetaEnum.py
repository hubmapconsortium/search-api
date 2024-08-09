from enum import Enum, EnumMeta

# Throw in extra classes to get the syntactic sugar an enumeration should have to support the 'in' operator.
# https://stackoverflow.com/a/10446010/1119928
# https://stackoverflow.com/a/65225753/1119928
class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True
