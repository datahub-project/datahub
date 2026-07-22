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

echo "TEST_STRATEGY: $TEST_STRATEGY, BATCH_COUNT: $BATCH_COUNT, BATCH_NUMBER: $BATCH_NUMBER"

# PYTEST_XDIST_WORKERS: optional pytest-xdist worker count (nightly sets this to 3).
# Independent of BATCH_COUNT/BATCH_NUMBER — matrix batching and xdist are orthogonal knobs.
# --dist=worksteal: idle workers pull queued tests from busy workers, so one slow class
# (e.g. the ~440s lifecycle suite) no longer strands a single worker while others idle.
# Trade-off vs loadscope: less locality → module-scoped fixtures may run on more workers
# and shared-resource races are likelier; relies on per-test isolation to stay safe.
# Policy mutators run in a second serial pytest invocation after non-mutators finish.
xdist_args=()
if [[ "${PYTEST_XDIST_WORKERS:-0}" =~ ^[1-9][0-9]*$ ]]; then
  echo "PYTEST_XDIST_WORKERS=${PYTEST_XDIST_WORKERS}: enabling pytest-xdist -n ${PYTEST_XDIST_WORKERS} --dist=worksteal"
  xdist_args=(-n "${PYTEST_XDIST_WORKERS}" --dist=worksteal)
fi

# pytest exit 5 = no tests collected (empty phase for this batch is OK).
_pytest_ok() {
  local rc=$1
  [[ "$rc" -eq 0 || "$rc" -eq 5 ]]
}

# Run non-mutator tests (optional xdist), then mutators serially.
# Batching in conftest always sees the full module set before the phase filter,
# so both invocations keep identical BATCH_NUMBER assignment.
run_pytest_policy_phases() {
  local junit_prefix=$1
  shift
  local extra_args=("$@")
  local rc1=0
  local rc2=0

  echo "=========================================="
  echo "SMOKE POLICY PHASE 1: non-mutator tests"
  echo "=========================================="
  set +e
  SMOKE_POLICY_PHASE=1 pytest -rP --durations=20 -vv --continue-on-collection-errors \
    ${xdist_args[@]+"${xdist_args[@]}"} \
    --junit-xml="${junit_prefix}-phase1.xml" \
    ${extra_args[@]+"${extra_args[@]}"}
  rc1=$?
  set -e
  if ! _pytest_ok "$rc1"; then
    echo "Phase 1 failed with exit code $rc1"
  fi

  echo "=========================================="
  echo "SMOKE POLICY PHASE 2: global_policy_mutator tests (serial)"
  echo "=========================================="
  set +e
  # No xdist: mutators must not run concurrently against shared policy state.
  SMOKE_POLICY_PHASE=2 pytest -rP --durations=20 -vv --continue-on-collection-errors \
    --reruns 1 --reruns-delay 1 \
    --junit-xml="${junit_prefix}-phase2.xml" \
    ${extra_args[@]+"${extra_args[@]}"}
  rc2=$?
  set -e
  if ! _pytest_ok "$rc2"; then
    echo "Phase 2 failed with exit code $rc2"
  fi

  _pytest_ok "$rc1" || return "$rc1"
  _pytest_ok "$rc2" || return "$rc2"
  return 0
}

# TEST_STRATEGY:
#   if set to pytests, runs all pytests, skips cypress tests(though cypress test launch is via  a pytest).
#   if set tp cypress, runs all cypress tests
#   if blank, runs all.
# When invoked via the github action, BATCH_COUNT and BATCH_NUMBER env vars are set to run a slice of those tests per
# worker for parallelism. docker-unified.yml generates a test matrix of pytests/cypress in batches. As number of tests
# increase, the batch_count config (in docker-unified.yml) may need adjustment.
if [[ "${TEST_STRATEGY}" == "pytests" ]]; then
  #pytests only - github test matrix runs pytests in one of the runners when applicable.
  run_pytest_policy_phases junit.smoke-pytests -k 'not test_run_cypress'
elif [[ "${TEST_STRATEGY}" == "cypress" ]]; then
  # run only cypress tests. The test inspects BATCH_COUNT and BATCH_NUMBER and runs only a subset of tests in that batch.
  # github workflow test matrix will invoke this in multiple runners for each batch.
  # Skipping the junit at the pytest level since cypress itself generates junits on a per-test basis. The pytest is a single test for all cypress
  # tests and isnt very helpful.
  # Cypress is launched from a single pytest module; xdist is not useful here.
  pytest -rP --durations=20 -vvs --continue-on-collection-errors tests/cypress/integration_test.py
else
  run_pytest_policy_phases junit.smoke-all
fi
