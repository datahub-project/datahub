# Docker Images
The easiest way to bring up and test DataHub is using DataHub [Docker](https://www.docker.com) images 
which are continuously deployed to [Docker Hub](https://hub.docker.com/u/linkedin) with every commit to repository.

* [linkedin/datahub-gms](https://cloud.docker.com/repository/docker/linkedin/datahub-gms/)
* [linkedin/datahub-frontend](https://cloud.docker.com/repository/docker/linkedin/datahub-frontend/)
* [linkedin/datahub-mae-consumer](https://cloud.docker.com/repository/docker/linkedin/datahub-mae-consumer/)
* [linkedin/datahub-mce-consumer](https://cloud.docker.com/repository/docker/linkedin/datahub-mce-consumer/)

Above Docker images are created for DataHub specific use. You can check subdirectories to check how those images are
generated via [Dockerbuild](https://docs.docker.com/engine/reference/commandline/build/) files or 
how to start each container using [Docker Compose](https://docs.docker.com/compose/). Other than these, DataHub depends
on below Docker images to be able to run:
* [**Kafka and Schema Registry**](kafka)
* [**Elasticsearch**](elasticsearch)
* [**MySQL**](mysql)

Local-built ingestion image allows you to create on an ad-hoc basis `metadatachangeevent` with Python script.
The pipeline depends on all the above images composing up.
* [**Ingestion**](ingestion)

## Prerequisites
You need to install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/).

## Quickstart
If you want to quickly try and evaluate DataHub by running all necessary Docker containers, you can check 
[Quickstart Guide](quickstart).