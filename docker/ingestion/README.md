# DataHub MetadataChangeEvent (MCE) Ingestion Docker Image

Refer to [DataHub Metadata Ingestion](../../metadata-ingestion/mce-cli) to have a quick understanding of the architecture and 
responsibility of this service for the DataHub.

## Build & Run
```
cd docker/ingestion && docker-compose up --build
```
This command will rebuild the docker image and start a container based on the image.

To start a container using an existing image, run the same command without the `--build` flag.

### Container configuration

#### Prerequisite Containers
Before starting `ingestion` container, `kafka`, `datahub-gms`, `mysql` and `datahub-mce-consumer` containers should already be up and running. 