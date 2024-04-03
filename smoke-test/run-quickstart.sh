#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

../gradlew :smoke-test:installDev
source venv/bin/activate

mkdir -p ~/.datahub/plugins/frontend/auth/
echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props

DATAHUB_SEARCH_IMAGE="${DATAHUB_SEARCH_IMAGE:=opensearchproject/opensearch}"
DATAHUB_SEARCH_TAG="${DATAHUB_SEARCH_TAG:=2.9.0}"
XPACK_SECURITY_ENABLED="${XPACK_SECURITY_ENABLED:=plugins.security.disabled=true}"
ELASTICSEARCH_USE_SSL="${ELASTICSEARCH_USE_SSL:=false}"
USE_AWS_ELASTICSEARCH="${USE_AWS_ELASTICSEARCH:=true}"

echo "DATAHUB_VERSION = $DATAHUB_VERSION"
DATAHUB_TELEMETRY_ENABLED=false  \
DOCKER_COMPOSE_BASE="file://$( dirname "$DIR" )" \
DATAHUB_SEARCH_IMAGE="$DATAHUB_SEARCH_IMAGE" DATAHUB_SEARCH_TAG="$DATAHUB_SEARCH_TAG" \
XPACK_SECURITY_ENABLED="$XPACK_SECURITY_ENABLED" ELASTICSEARCH_USE_SSL="$ELASTICSEARCH_USE_SSL" \
USE_AWS_ELASTICSEARCH="$USE_AWS_ELASTICSEARCH" \
datahub docker quickstart --version ${DATAHUB_VERSION} --standalone_consumers --dump-logs-on-failure --kafka-setup
