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
fi

source venv/bin/activate

(cd ..; ./gradlew :smoke-test:yarnInstall)

source ./set-cypress-creds.sh

# no_cypress_suite0, no_cypress_suite1, cypress_suite1, cypress_rest
if [[ -z "${TEST_STRATEGY}" ]]; then
    pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke.xml
else
    if [ "$TEST_STRATEGY" == "no_cypress_suite0" ]; then
        pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke_non_cypress.xml -k 'not test_run_cypress' -m 'not no_cypress_suite1'
    elif [ "$TEST_STRATEGY" == "no_cypress_suite1" ]; then
        pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke_non_cypress.xml -m 'no_cypress_suite1'
    else
        pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke_cypress_${TEST_STRATEGY}.xml tests/cypress/integration_test.py
    fi
fi
