# HuBMAP Search API

The HuBMAP Search API is a thin wrapper of the Elasticsearch. It handles data indexing and reindexing into the backend Elasticsearch. It also accepts the search query and passes through to the Elasticsearch with data access security check.

The API documentation is available on SmartAPI at https://smart-api.info/ui/7aaf02b838022d564da776b03f357158

## Updating the enumerations

All codes used by the system need to be represented in
[src/search-schema/data/definitions/](src/search-schema/data/definitions/).
The most frequently updated is `assay_types.yaml`:
Nils likes to review the descriptions here, so he is listed as a "[code owner](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/about-code-owners)".

## Search endpoint and group access check

The search-api base URL for each deployment environment:

- DEV: `https://search-api.dev.hubmapconsortium.org`
- TEST: `https://search-api.test.hubmapconsortium.org`
- STAGE: `https://search-api.stage.hubmapconsortium.org`
- PROD: `https://search.api.hubmapconsortium.org`

## Request endpoints

### Get all supported indices

This endpoint returns a list of supported indices, no globus token is required to make the request.

````
GET /indices
````

### Search without specifiing an index

The Authorization header with globus token is optional

````
GET/POST /search
````

### Search against a specified index

The Authorization header with globus token is optional

````
GET/POST /<index>/search
````
Due to data access restriction, indexed entries are protected and calls to the above endpoints require the `Authorization` header with the Bearer token (globus nexus token) along with the search query JSON body. There are three cases when making a search call:

- Case #1: Authorization header is missing, default to use the `entities` index with only public data entries. 
- Case #2: Authorization header with valid token, but the member doesn't belong to the HuBMAP-Read group, direct the call to use the `entities` index with only public data entries. 
- Case #3: Authorization header presents but with invalid or expired token, return 401 (if someone is sending a token, they might be expecting more than public stuff).
- Case #4: Authorization header presents with a valid token that has the group access, **ALL** the user specified search query DSL (Domain Specific Language) detail will be passed to the Elasticsearch just like making queries against the Elasticsearch directly.

NOTE: currently, the Search API doesn't support comma-separated list or wildcard expression of index names in the URL path used to limit the request.

## Query examples

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

### Aggregation 

````
{
  "aggs": {
    "created_by_user_displayname": {
      "filter": {
        "term": {
          "entity_type.keyword": "Dataset"
        }
      }
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

## Deploy with other HuBMAP docker compose projects

This option allows you to setup all the pieces in a containerized environment with docker and docker-compose. This requires to have the [HuBMAP Gateway](https://github.com/hubmapconsortium/gateway) running locally before starting building the Search API docker compose project. Please follow the [instructions](https://github.com/hubmapconsortium/gateway#workflow-of-setting-up-multiple-hubmap-docker-compose-projects). It also requires the Gateway project to be configured accordingly.

For local development (the localhost mode), this Docker Compose project also comes with Elasticsearch and Kibana.
