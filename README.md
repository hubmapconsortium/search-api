# HuBMAP Search API

## Overview of tools

- [Docker Engine](https://docs.docker.com/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

Note: Docker Compose requires Docker to be installed and running first.

## Setup local development with Elasticsearch and Kibana with Docker Compose

To start up the Elasticsearch and Kibana containers:

```
cd docker
sudo docker-compose up -d
```
## Usage

The search endpoint is
````
POST https://search-api.dev.hubmapconsortium.org/search
````
It's optional to use the `Authorization` header with the Bearer token (globus nexus token). If the token represents a user who has group access to the indexed data, the search API will pass the query to the backend elasticsearch server and return the search hits that match the query defined in the request. If a token is not present or invalid, only data marked as public will be returned.

Below is the sample JSON in the request. 

````
{
  "version": true,
  "size": 500,
  "sort": [
    {
      "_score": {
        "order": "desc"
      }
    }
  ],
  "_source": {
    "excludes": []
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [],
  "query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        }
      ],
      "should": [],
      "must_not": []
    }
  }
}
````

For a request with a valid token that resprents a member who belongs to the HuBMAP read group, the request JSON may narrow down the search with the `access_group` field, currently only "Open" and "Readonly" are the valid values.

````
{
  "version": true,
  "size": 500,
  "sort": [
    {
      "_score": {
        "order": "desc"
      }
    }
  ],
  "_source": {
    "excludes": []
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [],
  "query": {
    "bool": {
      "must": [
      {
            "match_phrase": {
                "access_group": {
                    "query": "Open"
                }
            }
        }],
      "filter": [
        {
          "match_all": {}
        }
      ],
      "should": [],
      "must_not": []
    }
  }
}
````