# Index Neo4j to ElasticSearch

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
