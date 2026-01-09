#!/bin/bash

cd "$(dirname "$0")/.."
set -euxo pipefail

# Note that requirements-local.in already includes `-e .`.
# We still need to specify pyproject.toml again in the dev/observe targets to tell
# it which package to get optional dependencies from.
# The --universal --python-version <version> flags are required to generate
# a lockfile that is compatible with Python 3.11+, regardless of what version
# the user has installed.
# --index-strategy unsafe-best-match ensures consistent resolution when
# UV_EXTRA_INDEX_URL is set (e.g., Cloudsmith in CI).

# Run full and slim builds in parallel (they're independent)
# Full build: requirements.txt WITH observe extra (prophet + observe-models)
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -o requirements.txt --extra observe pyproject.toml requirements-local.in $@ &
PID_FULL=$!

# Slim build: requirements-slim.txt WITHOUT observe extra (no prophet, no observe-models)
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -o requirements-slim.txt requirements-local.in $@ &
PID_SLIM=$!

# Wait for both to complete
wait $PID_FULL $PID_SLIM

# Dev build: requirements-dev.txt with dev extra (depends on requirements.txt)
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -o requirements-dev.txt --extra dev pyproject.toml requirements-local.in requirements.txt $@

./scripts/sync.sh
