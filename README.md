# HuBMAP Search API

The HuBMAP Search API is a thin wrapper of the Elasticsearch. It handles data indexing and reindexing into the backend Elasticsearch. It also accepts the search query and passes through to the Elasticsearch with data access security check.

The API documentation is available on SmartAPI at https://smart-api.info/ui/7aaf02b838022d564da776b03f357158

## Development process

### To release via TEST infrastructure
- Make new feature or bug fix branches from `test-release`.
- Make PRs to `test-release`. (This is the default branch.)
- As a codeowner, Zhou is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to TEST infrastructure, and redeploy and reindex the TEST instance.
- Developer or someone on the team who is familiar with the change will test/qa the change
- When any current changes in the `devel-test` have been approved after test/qa on TEST, Zhou will release to PROD.

### To work on features in the development environment before ready for testing and releasing
- Make new feature branches from `test-release`.
- Make PRs to `dev-integrate`.
- As a codeowner, Zhou is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to devel, and redeploy and reindex the DEV instance.
- When a feature branch is ready for testing and release, make a PR to test-release for deployment and testing on the TEST infrastructure as above.

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


### Get search result count without specifiing an index

Similar to making a request against `/search` but for getting the count:

````
GET /count
````

### Get search result count against a specified index

Similar to making a request against `/<index>/search` but for getting the count:

````
GET /<index>/count
````

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

### Live reindex

Trigger the reindex:

````
curl -i -X PUT -H "Authorization:Bearer <globus-nexus-token>" <search-api base URL>/reindex-all
````

The token will need to be in the admin group.

## Development and deployment environments

We have the following 5 development and deployment environments:

* localhost - all the services will be deployed with docker containers including sample Neo4j and sample MySQL are running on the same localhost listing on different ports, without globus data
* dev - all services except ingest-api will be running on AWS EC2 with SSL certificates, Neo4j and MySQL are dev versions on AWS, and ingest-api(and another nginx) will be running on PSC with domain and globus data
* test - similar to dev with a focus on testing and connects to Neo4j and MySQL test versions of database
* stage - as similar to the production environment as it can be.
* prod - similar to test but for production settings with production versions of Neo4j and MySQL

### Localhost development

This option allows you to setup all the pieces in a containerized environment with docker and docker-compose. This requires to have the [HuBMAP Gateway](https://github.com/hubmapconsortium/gateway) running locally before starting building this docker compose project. Please follow the [instructions](https://github.com/hubmapconsortium/gateway#workflow-of-setting-up-multiple-hubmap-docker-compose-projects). It also requires the Gateway project to be configured accordingly.

### Remote deployment

In localhost mode, all the docker containers are running on the same host machine. However, the ingest-api will be deployed on a separare host machine for dev, test, stage, and prod mode due to different deployment requirements. 

There are a few configurable environment variables to keep in mind:

- `COMMONS_BRANCH`: build argument only to be used during image creation. We can specify which [commons](https://github.com/hubmapconsortium/commons) branch to use during the image creation. Default to master branch if not set or null.
- `HOST_UID`: the user id on the host machine to be mapped to the container. Default to 1000 if not set or null.
- `HOST_GID`: the user's group id on the host machine to be mapped to the container. Default to 1000 if not set or null.

We can set and verify the environment variable like below:

````
export COMMONS_BRANCH=devel
echo $COMMONS_BRANCH
````

Note: Environment variables set like this are only stored temporally. When you exit the running instance of bash by exiting the terminal, they get discarded. So for rebuilding the docker image, we'll need to make sure to set the environment variables again if necessary.

````
Usage: ./search-api-docker.sh [localhost|dev|test|stage|prod] [check|config|build|start|stop|down]
````

Before we go ahead to start building the docker image, we can do a check to see if the required configuration file is in place:

````
cd docker
./search-api-docker.sh dev check
````

We can also validate and view the details of corresponding compose file:

````
./search-api-docker.sh dev config
````

Building the docker images and starting/stopping the contianers require to use docker daemon, you'll probably need to use `sudo` in the following steps. If you don’t want to preface the docker command with sudo, add users to the docker group:

````
sudo usermod -aG docker $USER
````

Then log out and log back in so that your group membership is re-evaluated. If testing on a virtual machine, it may be necessary to restart the virtual machine for changes to take effect.

To build the docker image of search-api:

````
./search-api-docker.sh dev build
````

To start up the search-api container:

````
./search-api-docker.sh dev start
````

And stop the running container by:

````
./search-api-docker.sh dev stop
````

You can also stop the running container and remove it by:

````
./search-api-docker.sh dev down
````

### Tweaks needed for Elasticsearch

Will need to increase the fields limit for each index. For example:
````
PUT hm_consortium_entities/_settings
{
  "index.mapping.total_fields.limit": 5000
}
````

### Updating API Documentation

The documentation for the API calls is hosted on SmartAPI.  Modifying the `search-api-spec.yaml` file and commititng the changes to github should update the API shown on SmartAPI.  SmartAPI allows users to register API documents.  The documentation is associated with this github account: api-developers@hubmapconsortium.org. Please contact Chuck Borromeo (chb69@pitt.edu) if you want to register a new API on SmartAPI.

