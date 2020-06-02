# DataHub MetadataAuditEvent (MAE) Consumer Docker Image
[![datahub-mae-consumer docker](https://github.com/linkedin/datahub/workflows/datahub-mae-consumer%20docker/badge.svg)](https://github.com/linkedin/datahub/actions?query=workflow%3A%22datahub-mae-consumer+docker%22)

Refer to [DataHub MAE Consumer Job](../../metadata-jobs/mae-consumer-job) to have a quick understanding of the architecture and 
responsibility of this service for the DataHub.

## Build & Run
```
cd docker/mae-consumer && docker-compose up --build
```
This command will rebuild the docker image and start a container based on the image.

To start a container using a previously built image, run the same command without the `--build` flag.

### Container configuration

#### Docker Network
All Docker containers for DataHub are supposed to be on the same Docker network which is `datahub_network`. 
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