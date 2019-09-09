# Data Hub MetadataAuditEvent (MAE) Consumer Docker Image
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/keremsahin/datahub-mae-consumer)](https://cloud.docker.com/repository/docker/keremsahin/datahub-mae-consumer/)

Refer to [Data Hub MAE Consumer Job](../../metadata-jobs/elasticsearch-index-job) to have a quick understanding of the architecture and 
responsibility of this service for the Data Hub.

## Build
```
docker image build -t keremsahin/datahub-mae-consumer -f docker/mae-consumer/Dockerfile .
```
This command will build and deploy the image in your local store.

## Run container
```
cd docker/mae-consumer && docker-compose up
```
This command will start the container. If you have the image available in your local store, this image will be used
for the container otherwise it will download the `latest` image from Docker Hub and then start that.

### Container configuration

#### Docker Network
All Docker containers for Data Hub are supposed to be on the same Docker network which is `datahub_network`. 
If you change this, you will need to change this for all other Docker containers as well.
```
networks:
  default:
    name: datahub_network
```

#### Elasticsearch and Kafka Containers
Before starting `datahub-mae-consumer` container, `elasticsearch` and `kafka` containers should already be up and running. 
These connections are configured via environment variables in `docker-compose.yml`:
```
environment:
  - KAFKA_BOOTSTRAP_SERVER=broker:29092
  - KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:8081
```
The value of `KAFKA_BOOTSTRAP_SERVER` variable should be set to the host name of the `kafka broker` container within the Docker network.
The value of `KAFKA_SCHEMAREGISTRY_URL` variable should be set to the host name of the `kafka schema registry` container within the Docker network.

```
environment:
  - ELASTICSEARCH_HOST=elasticsearch
  - ELASTICSEARCH_PORT=9200
```
The value of `ELASTICSEARCH_HOST` variable should be set to the host name of the `elasticsearch` container within the Docker network.