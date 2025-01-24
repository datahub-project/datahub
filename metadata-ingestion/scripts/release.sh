#!/bin/bash
set -euxo pipefail

if [[ ! ${RELEASE_SKIP_TEST:-} ]] && [[ ! ${RELEASE_SKIP_INSTALL:-} ]]; then
	../gradlew build  # also runs tests
elif [[ ! ${RELEASE_SKIP_INSTALL:-} ]]; then
	../gradlew install
fi

MODULE=datahub

# Check packaging constraint.
python -c 'import setuptools; where="./src"; assert setuptools.find_packages(where) == setuptools.find_namespace_packages(where), "you seem to be missing or have extra __init__.py files"'

# Update the release version.
if [[ ! ${RELEASE_VERSION:-} ]]; then
    echo "RELEASE_VERSION is not set"
    exit 1
fi
sed -i.bak "s/__version__ = \"1\!0.0.0.dev0\"/__version__ = \"$(echo $RELEASE_VERSION|sed s/-/+/)\"/" src/${MODULE}/_version.py

# Build and upload the release.
rm -rf build dist || true
python -m build
if [[ ! ${RELEASE_SKIP_UPLOAD:-} ]]; then
    python -m twine upload 'dist/*'
fi
mv src/${MODULE}/_version.py.bak src/${MODULE}/_version.py
