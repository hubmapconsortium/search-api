# HuBMAP Search API

The HuBMAP Search API is a thin wrapper of the Elasticsearch. It handles data indexing and reindexing into the backend Elasticsearch. It also accepts the search query and passes through to the Elasticsearch with data access security check.

## Search endpoint and group access check

The search endpoint for each deployment environment:

- DEV: `https://search-api.dev.hubmapconsortium.org/search`
- TEST: `https://search-api.test.hubmapconsortium.org/search`

Both HTTP `GET` and `POST` are supported. Due to data access restriction, some indexed entries are protected and require the `Authorization` header with the Bearer token (globus nexus token) along with the search query JSON body. With a valid token (that represents a user who has group access to the indexed data), **ALL** the user specified search query DSL (Domain Specific Language) detail will be passed to the Elasticsearch just like making queries against the Elasticsearch directly, and the search hits results will be returned.

If a token is missing or invalid, only public accessible data entries that have been indexed will be returned for the provided query. This is being done by modifying the orignal query JSON. As a result, not all queries defined by Elasticsearch are supported by this Search API. The following are the supported query clauses in this use case:

- Leaf query clauses: `match_all`, `match`, `match_phrase`, `term`, `terms`, `terms_set`, `range`, `exists`, `ids`, `type`, `prefix`, `match_phrase_prefix`, `match_bool_prefix`, `fuzzy`, `wildcard`, `regexp`
- Compound query clauses: `bool`, `dis_max`

NOTE: currently, the Search API doesn't support comma-separated list or wildcard expression of index names used to limit the request.

## Query examples

The request JSON body to this Search API must start with "query" element

````
{
    "query": {
         
    }
    ...
}
````

### Leaf query - match

````
{
    "query": {
        "match": {
            "uuid": "4cac248a51b6767e029663b273e7a8b2"
        }
    }
}
````

Note: 

### Compound query - bool

````
{
    "query": {
        "bool": {
            "must": [
                {
                    "match_phrase": {
                        "donor.group_name": "Vanderbilt TMC"
                    }
                }
            ],
            "filter": [
                {
                    "match": {
                        "origin_sample.entity_type": "Sample"
                    }
                }
            ]
        }
    }
}
````

For a request with a valid token that resprents a member who belongs to the HuBMAP read group, the request JSON may narrow down the hits with the `access_group` field, currently only "Open" and "Readonly" are the valid values.

````
{
    "query": {
        "term": {
            "access_group": "Readonly"
        }
    }
}
````

### With Python requests

```
query_dict = {
    'query': {
        'match': {
            'uuid': uuid
        }
    }
}
response = requests.post(
    'https://search-api.dev.hubmapconsortium.org/search',
    json = query_dict,
    headers = {'Authorization': 'Bearer ' + nexus_token})
hits = response.json()['hits']['hits']
```

Again, the `Authorization` is optional. Valid token in the header will allow all queries to be passed to the backend Elasticsearch. And missing or invalid token will only apply the search against the public accessible indexed data entries.

## Deploy with other HuBMAP docker compose projects

This option allows you to setup all the pieces in a containerized environment with docker and docker-compose. This requires to have the [HuBMAP Gateway](https://github.com/hubmapconsortium/gateway) running locally before starting building the Search API docker compose project. Please follow the [instructions](https://github.com/hubmapconsortium/gateway#workflow-of-setting-up-multiple-hubmap-docker-compose-projects). It also requires the Gateway project to be configured accordingly.

For local development (the localhost mode), this Docker Compose project also comes with Elasticsearch and Kibana.