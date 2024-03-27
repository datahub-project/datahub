#!/bin/bash

cd "$(dirname "$0")/.."

set -euxo pipefail

uv pip compile -o requirements.txt requirements-local.in
uv pip compile -o requirements-dev.txt --extra dev pyproject.toml requirements-local.in requirements.txt

# TODO: If in CI, throw an error if the requirement files aren't in sync with
# the package dependencies.

./scripts/sync.sh
