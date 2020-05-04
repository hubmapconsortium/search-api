# HuBMAP Search API

The HuBMAP Search API is a thin wrapper of the Elasticsearch. It handles data indexing and reindexing into the backend Elasticsearch. It also accepts the search query and passes through to the Elasticsearch with data access security check.

## Search endpoint and group access check

The search endpoint for each deployment environment:

- DEV: `https://search-api.dev.hubmapconsortium.org/search`
- TEST: `https://search-api.test.hubmapconsortium.org/search`

Both HTTP `GET` and `POST` methods are supported. Due to data access restriction, indexed entries are protected and calls to the above endpoint require the `Authorization` header with the Bearer token (globus nexus token) along with the search query JSON body. There are three cases when making a search call:

### Case 1: Missing/invalid/expired token

If a token is missing, invalid or expired, an error message with 401 status code will be returned. 

### Case 2: Valid token without the right group access

In the case that the token is valid **BUT** the owner of this token doesn't have the right group access, a 403 error message will be returned.

### Case 3: Valid token with right group access

With a valid token (that represents a user who has the correct group access to the indexed data), **ALL** the user specified search query DSL (Domain Specific Language) detail will be passed to the Elasticsearch just like making queries against the Elasticsearch directly, and the search hits results will be returned. Elasticsearch will also return any error messges if the JSON query is misformatted.

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