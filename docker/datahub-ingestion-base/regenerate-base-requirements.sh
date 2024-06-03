#!/bin/bash

# This script is used to regenerate the base-requirements.txt file

set -euxo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )"

SCRIPT_NAME=$(basename "$0")
DATAHUB_DIR=$(pwd)/../..

# Create a virtualenv.
VENV_DIR=$(mktemp -d)
python -c "import sys; assert sys.version_info >= (3, 9), 'Python 3.9 or higher is required.'"
python -m venv $VENV_DIR
source $VENV_DIR/bin/activate
pip install --upgrade pip uv setuptools wheel
echo "Using virtualenv at $VENV_DIR"

# Install stuff.
pushd $DATAHUB_DIR/metadata-ingestion
uv pip install -e '.[all]' -e '../metadata-ingestion-modules/airflow-plugin/[plugin-v2]'
popd

# Generate the requirements file.
# Removing Flask deps due as per https://github.com/datahub-project/datahub/pull/6867/files
# Removing py4j and PyJWT due to https://github.com/datahub-project/datahub/pull/6868/files
# Removing pyspark and pydeequ because we don't want them in the slim image, so they can be added separately.
# TODO: It's unclear if these removals are still actually needed.
echo "# Generated requirements file. Run ./$SCRIPT_NAME to regenerate." > base-requirements.txt
pip freeze \
    | grep -v -E "^-e" \
    | grep -v -E "^uv==" \
    | grep -v "Flask-" \
    | grep -v -E "(py4j|PyJWT)==" \
    | grep -v -E "(pyspark|pydeequ)==" \
    >> base-requirements.txt
