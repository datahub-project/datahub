#!/bin/bash

cd "$(dirname "$0")/.."
set -euxo pipefail

TMPDIR=$(mktemp -d)
cp requirements.txt ${TMPDIR}/requirements.txt
cp requirements-dev.txt ${TMPDIR}/requirements-dev.txt

# Note that requirements-local.in already includes `-e .`.
# We still need to specify pyproject.toml again in the dev target to tell
# it which package to get `[dev]` dependencies from.
# --index-strategy unsafe-best-match ensures consistent resolution when
# UV_EXTRA_INDEX_URL is set (e.g., Cloudsmith in CI).
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -q -o ${TMPDIR}/requirements.txt requirements-local.in
uv pip compile --universal --python-version 3.11 --index-strategy unsafe-best-match -q -o ${TMPDIR}/requirements-dev.txt --extra dev pyproject.toml requirements-local.in requirements.txt

sed -i.prev "s/-o .*requirements.txt/-o requirements.txt/g" ${TMPDIR}/requirements.txt
sed -i.prev "s/-o .*requirements-dev.txt/-o requirements-dev.txt/g" ${TMPDIR}/requirements-dev.txt

diff requirements.txt ${TMPDIR}/requirements.txt
diff requirements-dev.txt ${TMPDIR}/requirements-dev.txt
