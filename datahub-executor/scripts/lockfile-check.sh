#!/bin/bash

cd "$(dirname "$0")/.."
set -euxo pipefail

TMPDIR=$(mktemp -d)
cp requirements.txt ${TMPDIR}/requirements.txt
cp requirements-slim.txt ${TMPDIR}/requirements-slim.txt
cp requirements-dev.txt ${TMPDIR}/requirements-dev.txt

# Note that requirements-local.in already includes `-e .`.
# We still need to specify pyproject.toml again in the dev/observe targets to tell
# it which package to get optional dependencies from.
# --index-strategy unsafe-best-match ensures consistent resolution when
# UV_EXTRA_INDEX_URL is set (e.g., Cloudsmith in CI).

# Full build: requirements.txt WITH observe extra
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -q -o ${TMPDIR}/requirements.txt --extra observe pyproject.toml requirements-local.in

# Slim build: requirements-slim.txt WITHOUT observe extra AND without PySpark
# Temporarily modify pyproject.toml to use s3-slim instead of s3 (excludes unity-catalog too)
# Both s3 and unity-catalog pull in PySpark which we want to exclude from slim builds
cp pyproject.toml pyproject.toml.bak
sed 's/\[base,snowflake,bigquery,redshift,s3,unity-catalog\]/[base,snowflake,bigquery,redshift,s3-slim]/g' pyproject.toml.bak > pyproject.toml
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -q -o ${TMPDIR}/requirements-slim.txt requirements-local.in
mv pyproject.toml.bak pyproject.toml

# Dev build: requirements-dev.txt with dev extra
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -q -o ${TMPDIR}/requirements-dev.txt --extra dev pyproject.toml requirements-local.in requirements.txt

sed -i.prev "s/-o .*requirements.txt/-o requirements.txt/g" ${TMPDIR}/requirements.txt
sed -i.prev "s/-o .*requirements-slim.txt/-o requirements-slim.txt/g" ${TMPDIR}/requirements-slim.txt
sed -i.prev "s/-o .*requirements-dev.txt/-o requirements-dev.txt/g" ${TMPDIR}/requirements-dev.txt

diff requirements.txt ${TMPDIR}/requirements.txt
diff requirements-slim.txt ${TMPDIR}/requirements-slim.txt
diff requirements-dev.txt ${TMPDIR}/requirements-dev.txt
