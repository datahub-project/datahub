---
title: "Deploying with Docker"
hide_title: true
---

# Docker Images

## Prerequisites
You need to install [docker](https://docs.docker.com/install/) and
[docker-compose](https://docs.docker.com/compose/install/) (if using Linux; on Windows and Mac compose is included with
Docker Desktop).

Make sure to allocate enough hardware resources for Docker engine. Tested & confirmed config: 2 CPUs, 8GB RAM, 2GB Swap
area.

## Quickstart

The easiest way to bring up and test DataHub is using DataHub [Docker](https://www.docker.com) images 
which are continuously deployed to [Docker Hub](https://hub.docker.com/u/linkedin) with every commit to repository.

You can easily download and run all these images and their dependencies with our
[quick start guide](../docs/quickstart.md).

DataHub Docker Images:

* [linkedin/datahub-ingestion](https://hub.docker.com/r/linkedin/datahub-ingestion/tags?page=1&name=v) - This contains the Python CLI. If you are looking for docker image for every minor CLI release you can find them under [acryldata/datahub-ingestion](https://hub.docker.com/r/acryldata/datahub-ingestion/tags?page=1&name=v)
* [linkedin/datahub-gms](https://cloud.docker.com/repository/docker/linkedin/datahub-gms/)
* [linkedin/datahub-frontend-react](https://cloud.docker.com/repository/docker/linkedin/datahub-frontend-react/)
* [linkedin/datahub-mae-consumer](https://cloud.docker.com/repository/docker/linkedin/datahub-mae-consumer/)
* [linkedin/datahub-mce-consumer](https://cloud.docker.com/repository/docker/linkedin/datahub-mce-consumer/)

Dependencies:
* [Kafka, Zookeeper, and Schema Registry](kafka-setup)
* [Elasticsearch](elasticsearch-setup)
* [MySQL](mysql)
* [(Optional) Neo4j](neo4j)

### Ingesting demo data.

If you want to test ingesting some data once DataHub is up, use the `./docker/ingestion/ingestion.sh` script or `datahub docker ingest-sample-data`. See the [quickstart guide](../docs/quickstart.md) for more details.

## Using Docker Images During Development

See [Using Docker Images During Development](../docs/docker/development.md).

## Building And Deploying Docker Images

We use GitHub Actions to build and continuously deploy our images. There should be no need to do this manually; a
successful release on Github will automatically publish the images.

### Building images

> This is **not** our recommended development flow and most developers should be following the
> [Using Docker Images During Development](../docs/docker/development.md) guide.

To build the full images (that we are going to publish), you need to run the following:

```
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -p datahub build
```

This is because we're relying on builtkit for multistage builds. It does not hurt also set `DATAHUB_VERSION` to
something unique.

## Ember
To serve the legacy Ember UI, follow the instructions below.

> **Before continuing**: If you've already run a deploy script, don't forget to clear containers using `docker container prune`

### Serving Ember Only

#### All Containers 

Use the `quickstart-ember.sh` script to launch all containers in DataHub, including a frontend server that serves the Ember UI
```
./quickstart-ember.sh
```

#### The Bare Minimum
Run the following command to launch only the Ember server and its required dependencies

```
docker-compose -f docker-compose.ember.yml -f docker-compose.yml -f docker-compose.override.yml up datahub-frontend-ember
```

Once complete, navigate to `localhost:9001` in your browser to see the legacy Ember app.

### Serving React + Ember
If you'd like to serve the React and Ember UIs side-by-side, you can deploy the `datahub-frontend-ember` service manually.

#### All Containers

To deploy all DataHub containers, run the quickstart script:
```
./quickstart.sh
```

Next, deploy the container that serves the Ember UI:

```
docker-compose -f docker-compose.ember.yml -f docker-compose.yml -f docker-compose.override.yml up --no-deps datahub-frontend-ember
```

#### The Bare Minimum
First, start the React frontend server & its required dependencies:

```
docker-compose up datahub-frontend-react
```

Then, start the Ember frontend server & its required dependencies: 
```
docker-compose -f docker-compose.ember.yml -f docker-compose.yml -f docker-compose.override.yml up datahub-frontend-ember
```

Navigate to `localhost:9002/` to view the React app & `localhost:9001/` to view the legacy Ember app. 
