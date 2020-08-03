# Docker Images

## Prerequisites
You need to install [docker](https://docs.docker.com/install/) and
[docker-compose](https://docs.docker.com/compose/install/).

Make sure to allocate enough hardware resources for Docker engine. Tested & confirmed config: 2 CPUs, 8GB RAM, 2GB Swap
area.

## Quickstart

The easiest way to bring up and test DataHub is using DataHub [Docker](https://www.docker.com) images 
which are continuously deployed to [Docker Hub](https://hub.docker.com/u/linkedin) with every commit to repository.

You can easily download and run all these images and their dependencies with our [quick start guide](../quickstart.md).

DataHub Docker Images:

* [linkedin/datahub-gms](https://cloud.docker.com/repository/docker/linkedin/datahub-gms/)
* [linkedin/datahub-frontend](https://cloud.docker.com/repository/docker/linkedin/datahub-frontend/)
* [linkedin/datahub-mae-consumer](https://cloud.docker.com/repository/docker/linkedin/datahub-mae-consumer/)
* [linkedin/datahub-mce-consumer](https://cloud.docker.com/repository/docker/linkedin/datahub-mce-consumer/)

Dependencies:
* [**Kafka and Schema Registry**](kafka)
* [**Elasticsearch**](elasticsearch-setup)
* [**MySQL**](mysql)

### Ingesting demo data.

If you want to test ingesting some data once DataHub is up, see [**Ingestion**](ingestion/README.md).

## Using Docker Images During Development

See [Using Docker Images During Development](development.md).

## Building And Deploying Docker Images

We use GitHub actions to build and continuously deploy our images. There should be no need to do this manually; a
successful release on Github will automatically publish the images.
