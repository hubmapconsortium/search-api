# Index Neo4j to ElasticSearch

## How to run this script

1. Make sure python 3.7 or above installed.
2. Create a conf.py file in the same directory of this readme file, and add the following variable in the file.

```
# Neo4j connection
NEO4J_CONF = {'NEO4J_SERVER':'bolt://10.10.10.10:1234', 'NEO4J_USERNAME': 'neo4j', 'NEO4J_PASSWORD': 'P@ssw0rd'}

# AWS ElasticSearch Endpoint
ELASTICSEARCH_CONF = {'ELASTICSEARCH_DOMAIN_ENDPOINT': 'https://aws-elastic-search.us-east-1.es.amazonaws.com'}
```

Edit the content with you neo4j connection infomation and aws elasticsearch domain endpoint.

3. Install Pipenv (Optional but Recommanded)

Pipenv Install instruction https://pipenv.kennethreitz.org/en/latest/

After install Pipenv

```
#cd into the /search-api/elasticsearch directory

pipenv shell

pipenv install

# install hubmapconsortium/commons individually because it sits on github. #egg speficy the package name in your local development enviorment.
pipenv install -e git+git://github.com/hubmapconsortium/commons.git#egg=hubmap-commons
# if you are developing commons lib also, install it from  your local directory.
pipenv install -e ../commons (relative path to your commons)
```

4. Run the script

```
python main.py <index_name>
```
