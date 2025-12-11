#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

if [ "${RUN_QUICKSTART:-true}" == "true" ]; then
    source ./run-quickstart.sh
fi

set +x
echo "Activating virtual environment"
source venv/bin/activate
set -x

# set environment variables for the test
source ./set-test-env-vars.sh

LOAD_DATA=$(cat <<EOF
from conftest import build_auth_session, build_graph_client
from tests.cypress.integration_test import ingest_data

auth_session = build_auth_session()
ingest_data(auth_session, build_graph_client(auth_session))
EOF
)

echo -e "$LOAD_DATA" | python

cd tests/cypress
yarn install

source "$DIR/set-cypress-creds.sh"

if [ "${RUN_UI:-true}" == "true" ]; then
  npx cypress open \
     --env "ADMIN_DISPLAYNAME=$CYPRESS_ADMIN_DISPLAYNAME,ADMIN_USERNAME=$CYPRESS_ADMIN_USERNAME,ADMIN_PASSWORD=$CYPRESS_ADMIN_PASSWORD"
fi
