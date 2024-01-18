#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

MONITORING_COMPOSE=""
if [[ $MONITORING == true ]]; then
  MONITORING_COMPOSE="-f ${DIR}/monitoring/docker-compose.monitoring.yml"
fi

CONSUMERS_COMPOSE=""
if [[ $SEPARATE_CONSUMERS == true ]]; then
  CONSUMERS_COMPOSE="-f ${DIR}/docker-compose.consumers-without-neo4j.yml -f ${DIR}/docker-compose.consumers.dev.yml"
  if [[ $MONITORING == true ]]; then
    MONITORING_COMPOSE="-f ${DIR}/monitoring/docker-compose.monitoring.yml -f ${DIR}/monitoring/docker-compose.consumers.monitoring.yml"
  fi
fi

M1_COMPOSE=""
if [[ $(uname -m) == 'arm64' && $(uname) == 'Darwin' ]]; then
  M1_COMPOSE="-f ${DIR}/docker-compose-without-neo4j.m1.yml"
fi

# Launches dev instances of DataHub images. See documentation for more details.
# YOU MUST BUILD VIA GRADLE BEFORE RUNNING THIS.
cd "${DIR}/../.." && \
  COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 DOCKER_DEFAULT_PLATFORM="$(uname -m)" docker compose \
    -f "${DIR}/docker-compose-without-neo4j.yml" \
    -f "${DIR}/docker-compose-without-neo4j.override.yml" \
    -f "${DIR}/docker-compose.dev.yml" \
    $CONSUMERS_COMPOSE $MONITORING_COMPOSE $M1_COMPOSE pull \
&& \
  COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 DOCKER_DEFAULT_PLATFORM="$(uname -m)" docker compose -p datahub \
    -f "${DIR}/docker-compose-without-neo4j.yml" \
    -f "${DIR}/docker-compose-without-neo4j.override.yml" \
    -f "${DIR}/docker-compose.dev.yml" \
    $CONSUMERS_COMPOSE $MONITORING_COMPOSE $M1_COMPOSE up --build $@
