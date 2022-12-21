#!/bin/bash
set -euxo pipefail

if [[ ! ${RELEASE_SKIP_TEST:-} ]]; then
	../../gradlew build  # also runs tests
fi

MODULE=datahub_airflow_plugin

# Check packaging constraint.
python -c 'import setuptools; where="./src"; assert setuptools.find_packages(where) == setuptools.find_namespace_packages(where), "you seem to be missing or have extra __init__.py files"'
if [[ ${RELEASE_VERSION:-} ]]; then
    # Replace version with RELEASE_VERSION env variable
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/${MODULE}/__init__.py
else
    vim src/${MODULE}/__init__.py
fi

rm -rf build dist || true
python -m build
if [[ ! ${RELEASE_SKIP_UPLOAD:-} ]]; then
    python -m twine upload 'dist/*'
fi
git restore src/${MODULE}/__init__.py
