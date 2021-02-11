#!/bin/bash

# Quickstarts DataHub that serves React app
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR && docker-compose -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.react.yml pull && docker-compose -p datahub \
    -f docker-compose.yml \
    -f docker-compose.override.yml \
    -f docker-compose.react.yml \
    up \
    --scale datahub-frontend=0
