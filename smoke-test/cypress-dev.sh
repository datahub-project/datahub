#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

if [ "${RUN_QUICKSTART:-true}" == "true" ]; then
    source ./run-quickstart.sh
fi

source venv/bin/activate

python -c 'from tests.cypress.integration_test import ingest_data; ingest_data()'

cd tests/cypress
npm install

source ./set-cypress-creds.sh

npx cypress open