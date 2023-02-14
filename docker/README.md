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

Do not use `latest` or `debug` tags for any of the image as those are not supported and present only due to leagcy reasons. Please use `head` or tags specific for versions like `v0.8.40`. For production we recommend using version specific tags not `head`.

* [linkedin/datahub-ingestion](https://hub.docker.com/r/linkedin/datahub-ingestion/) - This contains the Python CLI. If you are looking for docker image for every minor CLI release you can find them under [acryldata/datahub-ingestion](https://hub.docker.com/r/acryldata/datahub-ingestion/).
* [linkedin/datahub-gms](https://hub.docker.com/repository/docker/linkedin/datahub-gms/).
* [linkedin/datahub-frontend-react](https://hub.docker.com/repository/docker/linkedin/datahub-frontend-react/)
* [linkedin/datahub-mae-consumer](https://hub.docker.com/repository/docker/linkedin/datahub-mae-consumer/)
* [linkedin/datahub-mce-consumer](https://hub.docker.com/repository/docker/linkedin/datahub-mce-consumer/)
* [acryldata/datahub-upgrade](https://hub.docker.com/r/acryldata/datahub-upgrade/)
* [linkedin/datahub-kafka-setup](https://hub.docker.com/r/acryldata/datahub-kafka-setup/)
* [linkedin/datahub-elasticsearch-setup](https://hub.docker.com/r/linkedin/datahub-elasticsearch-setup/)
* [acryldata/datahub-mysql-setup](https://hub.docker.com/r/acryldata/datahub-mysql-setup/)
* [acryldata/datahub-postgres-setup](https://hub.docker.com/r/acryldata/datahub-postgres-setup/)
* [acryldata/datahub-actions](https://hub.docker.com/r/acryldata/datahub-actions). Do not use `acryldata/acryl-datahub-actions` as that is deprecated and no longer used.

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

### Community Built Images

As the open source project grows, community members would like to contribute additions to the docker images. Not all contributions to the images can be accepted because those changes are not useful for all community members, it will increase build times, add dependencies and possible security vulns. In those cases this section can be used to point to `Dockerfiles` hosted by the community which build on top of the images published by the DataHub core team along with any container registry links where the result of those images are maintained.