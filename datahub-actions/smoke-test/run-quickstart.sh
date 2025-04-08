#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

../gradlew :smoke-test:installDev
source venv/bin/activate

mkdir -p ~/.datahub/plugins/frontend/auth/
echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props

echo "DATAHUB_ACTIONS_VERSION = $DATAHUB_ACTIONS_VERSION"

DATAHUB_TELEMETRY_ENABLED=false  \
ACTIONS_VERSION=${DATAHUB_ACTIONS_VERSION}  \
DATAHUB_ACTIONS_IMAGE=acryldata/datahub-actions-slim \
datahub docker quickstart

# After quickstart succeeds, we modify the docker-compose file to inject the env
# file variable
python inject_actions_env_file.py ~/.datahub/quickstart/docker-compose.yml

# Then we run quickstart again with the modified docker-compose file

DATAHUB_TELEMETRY_ENABLED=false \
ACTIONS_VERSION=${DATAHUB_ACTIONS_VERSION}  \
ACTIONS_ENV_FILE=`pwd`/tests/resources/actions.env  \
DATAHUB_ACTIONS_IMAGE=acryldata/datahub-actions-slim \
datahub docker quickstart -f ~/.datahub/quickstart/docker-compose.yml