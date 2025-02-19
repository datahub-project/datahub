#!/bin/bash

cd "$(dirname "$0")/.."
set -euxo pipefail

# Note that requirements-local.in already includes `-e .`.
# We still need to specify pyproject.toml again in the dev target to tell
# it which package to get `[dev]` dependencies from.
rm requirements.txt
rm requirements-dev.txt
uv pip compile --universal -o requirements.txt requirements-local.in $@
uv pip compile --universal -o requirements-dev.txt --extra dev pyproject.toml requirements-local.in requirements.txt $@

./scripts/sync.sh
