#!/bin/bash

# read -p "Desired datahub version, for example 'v0.10.1' (defaults to newest): " VERSION
VERSION=${DATAHUB_VERSION:-head}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
IMAGE=acryldata/datahub-upgrade:$DATAHUB_VERSION
cd $DIR && docker pull ${IMAGE} && docker run --env-file ./env/docker.env --network="datahub_network" ${IMAGE} "$@"
