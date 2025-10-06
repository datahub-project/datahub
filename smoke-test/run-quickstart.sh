#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

../gradlew :smoke-test:installDev
set +x
echo "Activating virtual environment"
source venv/bin/activate
set -x

mkdir -p ~/.datahub/plugins/frontend/auth/
echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props

echo "DATAHUB_VERSION = $DATAHUB_VERSION"
DATAHUB_SEARCH_IMAGE="${DATAHUB_SEARCH_IMAGE:=opensearchproject/opensearch}"
DATAHUB_SEARCH_TAG="${DATAHUB_SEARCH_TAG:=2.9.0}"
XPACK_SECURITY_ENABLED="${XPACK_SECURITY_ENABLED:=plugins.security.disabled=true}"
ELASTICSEARCH_USE_SSL="${ELASTICSEARCH_USE_SSL:=false}"
USE_AWS_ELASTICSEARCH="${USE_AWS_ELASTICSEARCH:=true}"

echo "DATAHUB_SEARCH_IMAGE = $DATAHUB_SEARCH_IMAGE"
echo "DATAHUB_SEARCH_TAG = $DATAHUB_SEARCH_TAG"
echo "XPACK_SECURITY_ENABLED = $XPACK_SECURITY_ENABLED"
echo "ELASTICSEARCH_USE_SSL = $ELASTICSEARCH_USE_SSL"
echo "USE_AWS_ELASTICSEARCH = $USE_AWS_ELASTICSEARCH"

THEME_V2_DEFAULT=false \
SHOW_HAS_SIBLINGS_FILTER=false \
SHOW_SEARCH_BAR_AUTOCOMPLETE_REDESIGN=false \
SHOW_INGESTION_PAGE_REDESIGN=false \
SHOW_HOME_PAGE_REDESIGN=false \
SEARCH_BAR_API_VARIANT=AUTOCOMPLETE_FOR_MULTIPLE \
DATAHUB_TELEMETRY_ENABLED=false \
DOCKER_COMPOSE_BASE="file://$( dirname "$DIR" )" \
DATAHUB_SEARCH_IMAGE="$DATAHUB_SEARCH_IMAGE" DATAHUB_SEARCH_TAG="$DATAHUB_SEARCH_TAG" \
XPACK_SECURITY_ENABLED="$XPACK_SECURITY_ENABLED" ELASTICSEARCH_USE_SSL="$ELASTICSEARCH_USE_SSL" \
USE_AWS_ELASTICSEARCH="$USE_AWS_ELASTICSEARCH" \
DATAHUB_VERSION=${DATAHUB_VERSION} \
ELASTICSEARCH_INDEX_BUILDER_REFRESH_INTERVAL_SECONDS=1 \
DATAHUB_ACTIONS_IMAGE=acryldata/datahub-actions \
DATAHUB_LOCAL_ACTIONS_ENV=`pwd`/test_resources/actions/actions.env  \
docker compose --project-directory ../docker/profiles --profile ${PROFILE_NAME:-quickstart-consumers} up -d --quiet-pull --wait --wait-timeout 900

