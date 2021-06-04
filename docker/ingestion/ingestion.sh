#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export DATAHUB_VERSION=${DATAHUB_VERSION:-9829576}
cd $DIR && docker-compose pull && docker-compose -p datahub up
