# DataHub Frontend Docker Image

[![datahub-frontend docker](https://github.com/datahub-project/datahub/workflows/datahub-frontend%20docker/badge.svg)](https://github.com/datahub-project/datahub/actions?query=workflow%3A%22datahub-frontend+docker%22)

Refer to [DataHub Frontend Service](../../datahub-frontend) to have a quick understanding of the architecture and 
responsibility of this service for the DataHub.

## Checking out DataHub UI

After starting your Docker container, you can connect to it by typing below into your favorite web browser:

If using React app:
```
http://localhost:9002
```

You can sign in with `datahub` as username and password.

## Build instructions

If you want to build the `datahub-frontend` Docker image yourself, you can run this command from the root directory of the DataHub repository you have locally (using Buildkit):

`DOCKER_BUILDKIT=1 docker build -t your_datahub_frontend -f ./docker/datahub-frontend/Dockerfile .`

Please note the final `.` and that the tag `your_datahub_frontend` is determined by you.
