#!/bin/bash

# Quickstarts DataHub by pulling all images from dockerhub and then running the containers locally. No images are
# built locally. Note: by default this pulls the latest (head) version; you can change this to a specific version by setting
# the DATAHUB_VERSION environment variable.
export DATAHUB_VERSION=${DATAHUB_VERSION:-9829576}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR && docker-compose pull && docker-compose -p datahub up
