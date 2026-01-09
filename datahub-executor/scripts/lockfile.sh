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

# Full build: requirements.txt WITH observe extra (prophet + observe-models)
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -o requirements.txt --extra observe pyproject.toml requirements-local.in $@

# Slim build: requirements-slim.txt WITHOUT observe extra AND without PySpark
# Temporarily modify pyproject.toml to use s3-slim instead of s3 (excludes unity-catalog too)
# Both s3 and unity-catalog pull in PySpark which we want to exclude from slim builds
cp pyproject.toml pyproject.toml.bak
sed 's/\[base,snowflake,bigquery,redshift,s3,unity-catalog\]/[base,snowflake,bigquery,redshift,s3-slim]/g' pyproject.toml.bak > pyproject.toml
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -o requirements-slim.txt requirements-local.in $@
mv pyproject.toml.bak pyproject.toml

# Dev build: requirements-dev.txt with dev extra (depends on requirements.txt)
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -o requirements-dev.txt --extra dev pyproject.toml requirements-local.in requirements.txt $@

./scripts/sync.sh
