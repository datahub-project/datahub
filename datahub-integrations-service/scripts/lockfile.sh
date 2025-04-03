#!/bin/bash

cd "$(dirname "$0")/.."
set -euxo pipefail

# Note that requirements-local.in already includes `-e .`.
# We still need to specify pyproject.toml again in the dev target to tell
# it which package to get `[dev]` dependencies from.
# The --universal --python-version <version> flags are required to generate
# a lockfile that is compatible with Python 3.10+, regardless of what version
# the user has installed.
uv pip compile --universal --python-version 3.10 -o requirements.txt requirements-local.in $@
uv pip compile --universal --python-version 3.10 -o requirements-dev.txt --extra dev pyproject.toml requirements-local.in requirements.txt $@

./scripts/sync.sh
