# DataHub Generalized Metadata Store (GMS) Docker Image
[![datahub-gms docker](https://github.com/linkedin/datahub/workflows/datahub-gms%20docker/badge.svg)](https://github.com/linkedin/datahub/actions?query=workflow%3A%22datahub-gms+docker%22)

Refer to [DataHub GMS Service](../../gms) to have a quick understanding of the architecture and 
responsibility of this service for the DataHub.


## Build & Run
```
cd docker/gms && docker-compose up --build
```
This command will rebuild the local docker image and start a container based on the image.

To start a container using an existing image, run the same command without the `--build` flag.

### Container configuration
#### External Port
If you need to configure default configurations for your container such as the exposed port, you will do that in
`docker-compose.yml` file. Refer to this [link](https://docs.docker.com/compose/compose-file/#ports) to understand
how to change your exposed port settings.
```
ports:
  - "8080:8080"
```

#### Docker Network
All Docker containers for DataHub are supposed to be on the same Docker network which is `datahub_network`. 
If you change this, you will need to change this for all other Docker containers as well.
```
networks:
  default:
    name: datahub_network
```

#### MySQL, Elasticsearch and Kafka Containers
Before starting `datahub-gms` container, `mysql`, `elasticsearch`, `neo4j` and `kafka` containers should already be up and running. 
These connections are configured via environment variables in `docker-compose.yml`:
```
environment:
  - EBEAN_DATASOURCE_USERNAME=datahub
  - EBEAN_DATASOURCE_PASSWORD=datahub
  - EBEAN_DATASOURCE_HOST=mysql:3306
  - EBEAN_DATASOURCE_URL=jdbc:mysql://mysql:3306/datahub
  - EBEAN_DATASOURCE_DRIVER=com.mysql.jdbc.Driver
```
The value of `EBEAN_DATASOURCE_HOST` variable should be set to the host name of the `mysql` container within the Docker network.

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

```
environment:
  - NEO4J_HOST=neo4j:7474
  - NEO4J_URI=bolt://neo4j
  - NEO4J_USERNAME=neo4j
  - NEO4J_PASSWORD=datahub
```
The value of `NEO4J_URI` variable should be set to the host name of the `neo4j` container within the Docker network.