# Local modules
from elasticsearch.indexer import Indexer

####################################################################################################
## Variables to initialize the indexer
####################################################################################################

# Open: Only entities can open to the public
# All: All entities
# original: directly from neo4j
# transformed: transformed by portal transform method
indices = """{
            'hm_public_entities': ('public','original'),
            'hm_consortium_entities': ('consortium', 'original'),
            'hm_public_portal': ('public', 'portal'),
            'hm_consortium_portal': ('consortium', 'portal')
            }"""

original_doc_type = 'original'
portal_doc_type = 'portal'

# AWS ElasticSearch Endpoint
# Works regardless of the trailing slash /
elasticsearch_url = ''

# URLs for talking to Entity API (default value used for docker deployment, no token needed)
# Don't use localhost since uuid-api is running on a different container
# Point to remote URL for non-docker development
# Works regardless of the trailing slash
entity_api_url = 'https://entity-api.refactor.hubmapconsortium.org'

# Globus app client ID and secret
app_client_id = ''
app_client_secret = ''

####################################################################################################
## Create indexer instance and run
####################################################################################################

if __name__ == "__main__":
    # Create an instance of the indexer
    indexer = Indexer(indices, original_doc_type, portal_doc_type, elasticsearch_url, entity_api_url, app_client_id, app_client_secret)
    
    # Delete existing indices and recreate indices then index everything
    indexer.main()