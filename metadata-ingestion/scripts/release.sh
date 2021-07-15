#!/bin/bash
set -euxo pipefail

if [[ ! ${RELEASE_SKIP_TEST:-} ]]; then
	../gradlew build  # also runs tests
fi

vim src/datahub/__init__.py

rm -rf build dist || true
python -m build
python -m twine upload 'dist/*'

git restore src/datahub/__init__.py
