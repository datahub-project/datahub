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

* [linkedin/datahub-gms](https://cloud.docker.com/repository/docker/linkedin/datahub-gms/)
* [linkedin/datahub-frontend](https://cloud.docker.com/repository/docker/linkedin/datahub-frontend/)
* [linkedin/datahub-mae-consumer](https://cloud.docker.com/repository/docker/linkedin/datahub-mae-consumer/)
* [linkedin/datahub-mce-consumer](https://cloud.docker.com/repository/docker/linkedin/datahub-mce-consumer/)

Dependencies:
* [Kafka, Zookeeper, and Schema Registry](kafka-setup)
* [Elasticsearch](elasticsearch-setup)
* [MySQL](mysql)
* [Neo4j](neo4j)

### Ingesting demo data.

If you want to test ingesting some data once DataHub is up, see [Ingestion](ingestion/README.md) for more details.

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

## React
You may wish to serve the incubating React UI instead of the Ember UI. To do so, follow the instructions below.

> **Before continuing**: If you've already run a deploy script, don't forget to clear containers using `docker container prune`

### Serving React Only

#### All Containers 

Use the `quickstart-react.sh` script to launch all containers in DataHub, including a frontend server that returns a React UI
```
./quickstart-react.sh
```

#### The Bare Minimum
Run the following command to launch only the React server and its required dependencies

```
docker-compose -f docker-compose.react.yml -f docker-compose.yml -f docker-compose.override.yml up datahub-frontend-react
```

Once complete, navigate to `localhost:9002/` in your browser to see the React app.

### Serving React + Ember
If you'd like to serve the React and Ember UIs side-by-side, you can deploy the `datahub-frontend-react` container manually.

#### All Containers

To deploy all DataHub containers, run the quickstart script:
```
./quickstart.sh
```

Next, deploy the container that serves the React UI:

```
docker-compose -f docker-compose.react.yml -f docker-compose.yml -f docker-compose.override.yml up --no-deps datahub-frontend-react
```

#### The Bare Minimum
First, start the Ember frontend server & its required dependencies:

```
docker-compose up datahub-frontend
```

Then, start the React frontend server & its required dependencies: 
```
docker-compose -f docker-compose.react.yml -f docker-compose.yml -f docker-compose.override.yml up datahub-frontend-react
```

Navigate to `localhost:9001/` to view the Ember app & `localhost:9002/` to view the React app. 
