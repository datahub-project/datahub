#!/bin/bash

# Launches dev instances of DataHub images. See documentation for more details.
# YOU MUST BUILD VIA GRADLE BEFORE RUNNING THIS.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR && \
  DOCKER_BUILDKIT=1 docker-compose \
    -f docker-compose.yml \
    -f docker-compose.override.yml \
    -f docker-compose.dev.yml \
    pull \
&& \
  DOCKER_BUILDKIT=1 docker-compose -p datahub \
    -f docker-compose.yml \
    -f docker-compose.override.yml \
    -f docker-compose.dev.yml \
    up