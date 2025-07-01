#!/bin/bash

# 직접 변수 설정
MODULE="datahub_airflow_plugin"
RELEASE_VERSION="0.15.0.5"

cd "$(dirname "$0")/.."

# Check packaging constraint.
python -c 'import setuptools; where="./src"; assert setuptools.find_packages(where) == setuptools.find_namespace_packages(where), "you seem to be missing or have extra __init__.py files"'

# Build and upload the release.
rm -rf build dist || true
python -m build
if [[ ! ${RELEASE_SKIP_UPLOAD:-} ]]; then
    python -m twine upload --repository nexus-hmc 'dist/*'
fi