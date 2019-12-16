# Data Hub MetadataChangeEvent (MCE) Ingestion Docker Image

Refer to [Data Hub Metadata Ingestion](../../metadata-ingestion/mce-cli) to have a quick understanding of the architecture and 
responsibility of this service for the Data Hub.

## Build
```
 docker build -t ingestion -f docker/ingestion/Dockerfile .
```
This command will build and deploy the image in your local store.

## Run container
```
 docker run --network host ingestion
```
This command will start the container. If you have the image available in your local store, this image will be used
for the container otherwise it will build the image from local repository and then start that.

### Container configuration

#### Kafka and Data Hub GMS Containers
Before starting `ingestion` container, `datahub-gms`, `kafka` and `datahub-mce-consumer` containers should already be up and running. 