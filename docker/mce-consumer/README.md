# DataHub MetadataChangeEvent (MCE) Consumer Docker Image
[![datahub-mce-consumer docker](https://github.com/linkedin/datahub/workflows/datahub-mce-consumer%20docker/badge.svg)](https://github.com/linkedin/datahub/actions?query=workflow%3A%22datahub-mce-consumer+docker%22)

Refer to [DataHub MCE Consumer Job](../../metadata-jobs/mce-consumer-job) to have a quick understanding of the architecture and 
responsibility of this service for the DataHub.

## Build
```
docker image build -t linkedin/datahub-mce-consumer -f docker/mce-consumer/Dockerfile .
```
This command will build and deploy the image in your local store.

## Run container
```
cd docker/mce-consumer && docker-compose pull && docker-compose up
```
This command will start the container. If you have the image available in your local store, this image will be used
for the container otherwise it will download the `latest` image from Docker Hub and then start that.

### Container configuration

#### Docker Network
All Docker containers for DataHub are supposed to be on the same Docker network which is `datahub_network`. 
If you change this, you will need to change this for all other Docker containers as well.
```
networks:
  default:
    name: datahub_network
```

#### Kafka and DataHub GMS Containers
Before starting `datahub-mce-consumer` container, `datahub-gms` and `kafka` containers should already be up and running. 
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
  - GMS_HOST=datahub-gms
  - GMS_PORT=8080
```
The value of `GMS_HOST` variable should be set to the host name of the `datahub-gms` container within the Docker network. 