# HuBMAP Search API

## Overview of tools needed for deployment

- [Docker Engine](https://docs.docker.com/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

Note: Docker Compose requires Docker to be installed and running first.

## Setup local development with Elasticsearch and Kibana with Docker Compose

To start up the Elasticsearch and Kibana containers:

```
cd docker
docker-compose up -d
```
## Search endpoint

The search endpoint for each deployment environment:

- DEV: `https://search-api.dev.hubmapconsortium.org/search`
- TEST: `https://search-api.test.hubmapconsortium.org/search`

Both HTTP `GET` and `POST` are supported. It's optional to use the `Authorization` header with the Bearer token (globus nexus token). If the token represents a user who has group access to the indexed data, the search API will pass the query to the backend elasticsearch server and return the search hits that match the query defined in the request. If a token is not present or invalid, only data marked as public will be returned for the provided query DSL (Domain Specific Language).

## Supported queries

The request JSON body to this Search API must start with "query" element

````
{
    "query": {
         
    }
    ...
}
````

Not all queries defined by Elasticsearch are supported by this Search API, and following is a list of supported query clauses:

- Leaf query clauses: `match_all`, `match`, `match_phrase`, `term`
- Compound query clauses: `bool`, `dis_max`

## Usage examples

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
        "match": {
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
