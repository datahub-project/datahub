#!/bin/bash

cd "$(dirname "$0")/.."

set -euxo pipefail

uv pip compile -o requirements.txt requirements-local.in
uv pip compile -o requirements-dev.txt --extra dev pyproject.toml requirements-local.in requirements.txt

./scripts/sync.sh
