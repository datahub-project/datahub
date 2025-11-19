#!/bin/bash
set -euxo pipefail

# add additional paths of tests that work in remote mode. Most current smoke pytests dont work in remote mode.
pytest -rP --durations=20 -vvs --log-cli-level=INFO --tb=long --showlocals --continue-on-collection-errors -m "remote_tests or read_only" tests/
