#!/bin/bash
set -euxo pipefail

# Runs a basic e2e test. It is not meant to be fully comprehensive,
# but rather should catch obvious bugs before they make it into prod.
#
# Script assumptions:
#   - The gradle build has already been run.
#   - Python 3.6+ is installed and in the PATH.

# Log the locally loaded images
# docker images | grep "datahub-"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

if [ "${RUN_QUICKSTART:-true}" == "true" ]; then
    source ./run-quickstart.sh
else
  mkdir -p ~/.datahub/plugins/frontend/auth/
  echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props
  echo "datahub:datahub" > ~/.datahub/plugins/frontend/auth/user.props

  python3 -m venv venv
  set +x
  source venv/bin/activate
  set -x
  python -m pip install --upgrade 'uv>=0.1.10'
  uv pip install -r requirements.txt
fi

(cd ..; ./gradlew :smoke-test:yarnInstall)

source ./set-cypress-creds.sh

# set environment variables for the test
source ./set-test-env-vars.sh

# TEST_STRATEGY:
#   if set to pytests, runs all pytests, skips cypress tests(though cypress test launch is via  a pytest).
#   if set tp cypress, runs all cypress tests
#   if blank, runs all.
# When invoked via the github action, BATCH_COUNT and BATCH_NUM env vars are set to run a slice of those tests per
# worker for parallelism. docker-unified.yml generates a test matrix of pytests/cypress in batches. As number of tests
# increase, the batch_count config (in docker-unified.yml) may need adjustment.
if [[ "${TEST_STRATEGY}" == "pytests" ]]; then
  #pytests only - github test matrix runs pytests in one of the runners when applicable.
  pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke-pytests.xml -k 'not test_run_cypress'
elif [[ "${TEST_STRATEGY}" == "cypress" ]]; then
  # run only cypress tests. The test inspects BATCH_COUNT and BATCH_NUMBER and runs only a subset of tests in that batch.
  # github workflow test matrix will invoke this in multiple runners for each batch.
  pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke-cypress${BATCH_NUMBER}.xml tests/cypress/integration_test.py
else
  pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke-all.xml
fi
