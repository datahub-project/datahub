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
