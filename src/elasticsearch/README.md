# Index Neo4j to ElasticSearch

## About `mapper_metadata.VERSION`

The file named `mapper_metadata.VERSION` in this current directory is used to keep tracking the the version of the indexed data entries in Elasticsearch. The portal-ui also queries this version number from Elasticsearch and shows it at `https://portal.hubmapconsortium.org/dev-search`. Ensuring the version number consitency between the deployed search-api code and the one shows up in portal-ui is critical for data integrity purposes. Before the indexer code reindexes the data from Neo4j, we should increment this version number to indicte this reindexing. 

## How to run this script

1. Make sure python 3.6 or above installed.
2. Create a conf.ini file in the same directory of this readme file, use the conf.example.ini as example.

Edit the content with you elasticsearch domain endpoint and entity-api URL.

3. Install Pipenv (Optional but Recommanded)

Pipenv Install instruction https://pipenv.kennethreitz.org/en/latest/

After install Pipenv

```
#cd into the /search-api/elasticsearch directory

pipenv shell

# install the src/requirements.txt and setup.py 
pipenv install -e ./src

# install hubmapconsortium/commons individually because it sits on github. #egg speficy the package name in your local development enviorment.
pipenv install -e git+git://github.com/hubmapconsortium/commons.git#egg=hubmap-commons
# if you are developing commons lib also, install it from  your local directory.
pipenv install -e ../commons (relative path to your commons)
```
4. Configure the conf.ini file.
- Configure [ELASTICSEARCH] section.
- COnfigure [INDEX] section.

5. Run the script
Different env will create different number of replica shard.

```
python indexer.py [DEV|TEST|STAGE|PROD]
```

## To debug

Capture one or more documents which fail during indexing. Then, from `src/` run:
```
PYTHONPATH=. elasticsearch/debug.py ~/failing-doc-1.yaml ~/failing-doc-2.json ...
```
