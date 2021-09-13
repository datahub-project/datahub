#!/bin/bash

# Quickstarts an Ember-serving variant of DataHub by pulling all images from dockerhub and then running the containers locally.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export DATAHUB_VERSION=${DATAHUB_VERSION:-head}
cd $DIR && docker-compose -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.ember.yml pull && docker-compose -p datahub \
    -f docker-compose.yml \
    -f docker-compose.override.yml \
    -f docker-compose.ember.yml \
    up \
    --scale datahub-frontend-react=0
