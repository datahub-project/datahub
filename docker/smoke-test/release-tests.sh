#!/bin/bash
set -euxo pipefail

# Default values for pytest configuration
PYTEST_MARKERS="${PYTEST_MARKERS:-release_tests or read_only}"
PYTEST_TEST_PATHS="${PYTEST_TEST_PATHS:-tests/read_only tests/release_tests tests/metrics/test_metrics.py}"
PYTEST_EXTRA_ARGS="${PYTEST_EXTRA_ARGS:-}"

# add additional paths of tests that work in remote mode. Most current smoke pytests dont work in remote mode. 
# TODO (devashish.chandra) Add specific markers for remote enabled tests
# shellcheck disable=SC2086
pytest -ra --junit-prefix=release_tests --log-cli-level=INFO --continue-on-collection-errors -m "${PYTEST_MARKERS}" ${PYTEST_TEST_PATHS} ${PYTEST_EXTRA_ARGS}
