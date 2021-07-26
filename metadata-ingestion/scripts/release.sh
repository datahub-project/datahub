#!/bin/bash
set -euxo pipefail

if [[ ! ${RELEASE_SKIP_TEST:-} ]]; then
	../gradlew build  # also runs tests
fi

# Check packaging constraint.
python -c 'import setuptools; where="./src"; assert setuptools.find_packages(where) == setuptools.find_namespace_packages(where), "you seem to be missing or have extra __init__.py files"'

vim src/datahub/__init__.py

rm -rf build dist || true
python -m build
python -m twine upload 'dist/*'

git restore src/datahub/__init__.py
