#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
IMAGE=acryldata/datahub-upgrade:head
cd $DIR && docker pull ${IMAGE} && docker run --env-file ./env/docker.env --network="datahub_network" ${IMAGE} "$@"
