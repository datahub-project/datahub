# DataHub MetadataChangeEvent (MCE) Ingestion Docker Image

Refer to [DataHub Metadata Ingestion](../../metadata-ingestion/mce-cli) to have a quick understanding of the architecture and 
responsibility of this service for the DataHub.

## Build
```
 docker build -t ingestion -f docker/ingestion/Dockerfile .
```
This command will build and deploy the image in your local store.

## Run container
```
 cd docker/ingestion && docker-compose up
```
This command will start the container. If you have the image available in your local store, this image will be used
for the container otherwise it will build the image from local repository and then start that.

### Container configuration

#### Prerequisite Containers
Before starting `ingestion` container, `kafka`, `datahub-gms`, `mysql` and `datahub-mce-consumer` containers should already be up and running. 