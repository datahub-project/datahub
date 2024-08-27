#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

# Tear down and clean up all DataHub-related containers, volumes, and network
docker compose -p datahub down -v
docker compose rm -f -v

# Tear down ingestion container
(cd ingestion && docker compose -p datahub down -v)
