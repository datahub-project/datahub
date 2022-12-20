#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt

mkdir -p ~/.datahub/plugins/frontend/auth/
echo "test_user:test_pass" > ~/.datahub/plugins/frontend/auth/user.props

echo "DATAHUB_VERSION = ${DATAHUB_VERSION:=acryl-datahub 0.0.0.dev0}"
DATAHUB_TELEMETRY_ENABLED=false \
DOCKER_COMPOSE_BASE="file://$( dirname "$DIR" )" \
datahub docker quickstart --build-locally --standalone_consumers --dump-logs-on-failure

python -c 'from tests.cypress.integration_test import ingest_data; ingest_data()'

cd tests/cypress
npm install

export CYPRESS_ADMIN_USERNAME=${ADMIN_USERNAME:-test_user}
export CYPRESS_ADMIN_PASSWORD=${ADMIN_PASSWORD:-test_pass}

npx cypress open