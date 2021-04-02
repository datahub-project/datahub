#!/bin/bash

# A convenience command to run the built docker container with a local config file and local output directory.
# Note: this works by mounting the current working directory as a Docker volume.

set -euo pipefail

DOCKER_IMAGE=linkedin/datahub-ingestion:${DATAHUB_VERSION:-latest}

docker pull --quiet $DOCKER_IMAGE

docker run --rm \
    --network host \
    --workdir=/dir \
    --mount type=bind,source="$(pwd)",target=/dir \
    $DOCKER_IMAGE \
    $@
