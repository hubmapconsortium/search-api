# Note: The goal is to support free-text search.
# This is a stop-gap until mappings can be specified, and copy_to used:
# https://github.com/hubmapconsortium/search-api/issues/63

def add_everything(doc):
    doc['everything'] = []
