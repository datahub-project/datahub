#!/bin/bash
set -ex

# Handle the case when docker is aliased to podman or another container runtime
shopt -s expand_aliases
source ~/.bashrc

MONITORING_COMPOSE=""
if [[ $MONITORING == true ]]; then
  MONITORING_COMPOSE="-f monitoring/docker-compose.monitoring.yml"
fi

CONSUMERS_COMPOSE=""
if [[ $SEPARATE_CONSUMERS == true ]]; then
  CONSUMERS_COMPOSE="-f docker-compose.consumers-without-neo4j.yml -f docker-compose.consumers.dev.yml"
  if [[ $MONITORING == true ]]; then
    MONITORING_COMPOSE="-f monitoring/docker-compose.monitoring.yml -f monitoring/docker-compose.consumers.monitoring.yml"
  fi
fi

M1_COMPOSE=""
if [[ $(uname -m) == 'arm64' && $(uname) == 'Darwin' ]]; then
  M1_COMPOSE="-f docker-compose.m1.yml"
fi

# Launches dev instances of DataHub images. See documentation for more details.
# YOU MUST BUILD VIA GRADLE BEFORE RUNNING THIS.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR && \
  COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 DOCKER_DEFAULT_PLATFORM="$(uname -m)" docker compose \
    -f docker-compose-with-cassandra.yml \
    -f docker-compose.dev.yml \
    $CONSUMERS_COMPOSE $MONITORING_COMPOSE $M1_COMPOSE \
    pull \
&& \
  COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 DOCKER_DEFAULT_PLATFORM="$(uname -m)" docker compose -p datahub \
    -f docker-compose-with-cassandra.yml \
    -f docker-compose.dev.yml \
    $CONSUMERS_COMPOSE $MONITORING_COMPOSE $M1_COMPOSE \
    up --build $@
