# HuBMAP Search API

The HuBMAP Search API is a thin wrapper of the Elasticsearch. It handles data indexing and reindexing into the backend Elasticsearch. It also accepts the search query and passes through to the Elasticsearch with data access security check.

The API documentation is available on SmartAPI at https://smart-api.info/ui/7aaf02b838022d564da776b03f357158

## Working with submodule

This repository relies on the [search-adaptor](https://github.com/dbmi-pitt/search-adaptor) as a submodule to function. The file `.gitmodules` contains the configuration for the URL and specific branch of the submodule that is to be used. Once you already have cloned this repository and switched to the target branch, to load the latest `search-adaptor` submodule:

```
git submodule update --init --remote
```

## Development process

### Portal index

Front end developers who need to work on the `portal` index should start in
[the `addl_index_transformations/portal` subdirectory](https://github.com/hubmapconsortium/search-api/tree/main/hubmap-translation/src/hubmap_translation/addl_index_transformations/portal);
You don't need to read the rest of this page.

### Local development
After checking out the repo, installing the dependencies,
and starting a local Elasticsearch instance, tests should pass:
```shell
pip install -r src/requirements.txt
pip install -r src/requirements-dev.txt

# on mac:
brew tap elastic/tap
brew install elastic/tap/elasticsearch-full
elasticsearch &  # Wait for it to start...

./test.sh
```

### To release via TEST infrastructure
- Make new feature or bug fix branches from `main` branch (the default branch)
- Make PRs to `main`
- As a codeowner, Zhou (github username `yuanzhou`) is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to TEST infrastructure, and redeploy and reindex the TEST instance.
- Developer or someone on the team who is familiar with the change will test/qa the change
- When any current changes in the `main` have been approved after test/qa on TEST, Zhou will release to PROD using the same docker image that has been tested on TEST infrastructure.

### To work on features in the development environment before ready for testing and releasing
- Make new feature branches off the `main` branch
- Make PRs to `dev-integrate`
- As a codeowner, Zhou is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to devel, and redeploy and reindex the DEV instance.
- When a feature branch is ready for testing and release, make a PR to `main` for deployment and testing on the TEST infrastructure as above.

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
POST /search
````

### Search against a specified index

The Authorization header with globus token is optional

````
POST /<index>/search
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

### Configuring a Single or Multiple Indices

All configuration options reside within the **src/instance/search-config.yaml** file which will allow you to specify configuration options for each index, that will be available via the **search-api**. This file will support any number of index configurations.  

To get started, copy the **src/instance/search-config.yaml.example**.  Use this as a template to further defined your index. Here's a sample of a defined index, see that options explained in further detail below: 


```
default_index: my-index
indices:
  my-index: 
    active: true
    public: my-index-public 
    private: my-index-private
    document_source_endpoint: https://my-document-base
    elasticsearch:
      url: https://localhost:9200
      mappings: "default-config.yaml"
```

**Options**

**default_index: [index name]**

If you have multiple indices you need to specify which of these is the default.  By specifying this, a call to the base **/search** endpoint, without specifying an index name, will use this index as the default.  This should be specified even for single index definitions 

**indices:**  All indices definitions start after this declaration

**active: [true or false]** - designated if the index should be active (true) or inactive

**public: [index name]** - this will allow you to specify an index that contains data only viewable by non-authenticated users or usage for public facing endpoints

**private: [index name]** - this will allow you to specify an index that contains private data viewable only by a certain group, consortium or more specifically,  authenticated users.  

*Note: the public and private indices should be the same index name if you only have a single index*

**document_source_endpoint: [url]** (optional) - this will allow you to configure a document source (i.e., entities) which will be used by the indexer to populate your index from an alternate document store

**elasticsearch:** configurations specific to elasticsearch after this declaration 

**url: [url]** - url to the server hosting Elasticsearch

**mappings: [file]** - used to specify a file which contains index settings or mappings (i.e., mapping.total_fields.limit: 5000) specific to Elasticsearch. see [index settings](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html).  Also, the default settings are located in **elasticsearch/search-default-config.yaml**.  This file is used during the index creation process before data is ingested by the indexer.


## Docker build for local/DEV development

There are a few configurable environment variables to keep in mind:

- `COMMONS_BRANCH`: build argument only to be used during image creation when we need to use a branch of commons from github rather than the published PyPI package. Default to master branch if not set or null.
- `HOST_UID`: the user id on the host machine to be mapped to the container. Default to 1001 if not set or null.
- `HOST_GID`: the user's group id on the host machine to be mapped to the container. Default to 1001 if not set or null.

We can set and verify the environment variable like below:

````
export COMMONS_BRANCH=master
echo $COMMONS_BRANCH
````

Note: Environment variables set like this are only stored temporally. When you exit the running instance of bash by exiting the terminal, they get discarded. So for rebuilding the docker image, we'll need to make sure to set the environment variables again if necessary.

```
cd docker
./docker-development.sh [check|config|build|start|stop|down]
```

## Docker deployment for TEST/STAGE/PROD

On TEST/STAGE/PROD environments, we use the same published docker image from DockerHub for deployment rather than building a new image.

```
cd docker
./docker-deployment.sh [start|stop|down]
```

For the Release candicate (RC) instance use a separate script:

```
./docker-rc.sh [start|stop|down]
```

## Updating API Documentation

The documentation for the API calls is hosted on SmartAPI.  Modifying the `search-api-spec.yaml` file and commititng the changes to github should update the API shown on SmartAPI. SmartAPI allows users to register API documents. The documentation is associated with this github account: api-developers@hubmapconsortium.org.


