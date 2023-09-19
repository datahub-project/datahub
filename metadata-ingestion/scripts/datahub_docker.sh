#!/bin/bash

# A convenience command to run the built docker container with a local config file and local output directory.
# Note: this works by mounting the current working directory as a Docker volume.

set -euo pipefail

DOCKER_IMAGE=acryldata/datahub-ingestion:${DATAHUB_VERSION:-head}

echo "+ Pulling $DOCKER_IMAGE"
docker pull $DOCKER_IMAGE

echo '+ Running ingestion'
docker run --rm \
    --network host \
    --workdir=/dir \
    --mount type=bind,source="$(pwd)",target=/dir \
    $DOCKER_IMAGE \
    $@
