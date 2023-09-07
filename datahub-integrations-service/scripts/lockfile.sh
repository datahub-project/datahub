#!/bin/bash

set -euxo pipefail

pip-compile -o requirements.txt pyproject.toml requirements-local.in
pip-compile --extra dev -o requirements-dev.txt pyproject.toml requirements-local.in requirements.txt

# TODO: If in CI, throw an error if the requirement files aren't in sync with
# the package dependencies.
