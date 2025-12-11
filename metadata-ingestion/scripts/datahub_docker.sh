#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


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
