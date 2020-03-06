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

To solve the import error

```
pip install -e .
```
