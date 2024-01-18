#!/bin/bash

MONITORING_COMPOSE=""
if [[ $MONITORING == true ]]; then
  MONITORING_COMPOSE="-f quickstart/docker-compose.monitoring.quickstart.yml"
fi

CONSUMERS_COMPOSE=""
if [[ $SEPARATE_CONSUMERS == true ]]; then
  CONSUMERS_COMPOSE="-f docker-compose.consumers.yml"
fi

# Quickstarts DataHub by pulling all images from dockerhub and then running the containers locally. No images are
# built locally.
# Note: by default this pulls the latest (head) version or the tagged version if you checked out a release tag.
# You can change this to a specific version by setting the DATAHUB_VERSION environment variable.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


# Detect if this is a checkout of a tagged branch.
# If this is a tagged branch, use the tag as the default, otherwise default to head.
# If DATAHUB_VERSION is set, it takes precedence.
TAG_VERSION=$(cd $DIR && git name-rev --name-only --tags HEAD)
DEFAULT_VERSION=$(echo $TAG_VERSION | sed 's/undefined/head/')
export DATAHUB_VERSION=${DATAHUB_VERSION:-${DEFAULT_VERSION}}

M1_COMPOSE=""
if [[ $(uname -m) == 'arm64' && $(uname) == 'Darwin' ]]; then
  M1_COMPOSE="-f docker-compose.m1.yml"
fi

echo "Quickstarting DataHub: version ${DATAHUB_VERSION}"
if docker volume ls | grep -c -q datahub_neo4jdata
then
  echo "Datahub Neo4j volume found, starting with neo4j as graph service"
  cd $DIR && docker compose pull && docker compose -p datahub up
else
  echo "No Datahub Neo4j volume found, starting with elasticsearch as graph service"
  cd $DIR && \
  DOCKER_DEFAULT_PLATFORM="$(uname -m)" docker compose -p datahub \
    -f quickstart/docker-compose-without-neo4j.quickstart.yml \
    $MONITORING_COMPOSE $CONSUMERS_COMPOSE $M1_COMPOSE up $@
fi
