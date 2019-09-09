# Data Hub Frontend Docker Image
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/keremsahin/datahub-frontend)](https://cloud.docker.com/repository/docker/keremsahin/datahub-frontend/)

Refer to [Data Hub Frontend Service](../../datahub-frontend) to have a quick understanding of the architecture and 
responsibility of this service for the Data Hub.

## Build
```
docker image build -t keremsahin/datahub-frontend -f docker/frontend/Dockerfile .
```
This command will build and deploy the image in your local store.

## Run container
```
cd docker/frontend && docker-compose up
```
This command will start the container. If you have the image available in your local store, this image will be used
for the container otherwise it will download the `latest` image from Docker Hub and then start that.

### Container configuration
#### External Port
If you need to configure default configurations for your container such as the exposed port, you will do that in
`docker-compose.yml` file. Refer to this [link](https://docs.docker.com/compose/compose-file/#ports) to understand
how to change your exposed port settings.
```
ports:
  - "9001:9001"
```

#### Docker Network
All Docker containers for Data Hub are supposed to be on the same Docker network which is `datahub_network`. 
If you change this, you will need to change this for all other Docker containers as well.
```
networks:
  default:
    name: datahub_network
```

#### datahub-gms Container
Before starting `datahub-frontend` container, `datahub-gms` container should already be up and running. 
`datahub-frontend` service creates a connection to `datahub-gms` service and this is configured with environment 
variables in `docker-compose.yml`:
```
environment:
  - DATAHUB_GMS_HOST=datahub-gms
  - DATAHUB_GMS_PORT=8080
```
The value of `DATAHUB_GMS_HOST` variable should be set to the host name of the `datahub-gms` container within the Docker network. 

## Checking out Data Hub UI
After starting your Docker container, you can connect to it by typing below into your favorite web browser:
```
http://localhost:9001
```
To be able to sign in, you need to provide your user name. You don't need to type any password.