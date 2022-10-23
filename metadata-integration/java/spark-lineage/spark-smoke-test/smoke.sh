#!/bin/bash

set -e
# Script assumptions:
#   - The gradle build has already been run.
#   - Python 3.6+ is installed and in the PATH.
#   - pytest is installed
#   - requests is installed

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt

echo "--------------------------------------------------------------------"
echo "Setting up datahub server"
echo "--------------------------------------------------------------------"


pwd ../../../

function abspath() {
    # generate absolute path from relative path
    # $1     : relative filename
    # return : absolute path
    if [ -d "$1" ]; then
        # dir
        (cd "$1"; pwd)
    elif [ -f "$1" ]; then
        # file
        if [[ $1 = /* ]]; then
            echo "$1"
        elif [[ $1 == */* ]]; then
            echo "$(cd "${1%/*}"; pwd)/${1##*/}"
        else
            echo "$(pwd)/$1"
        fi
    fi
}

DATAHUB_TELEMETRY_ENABLED=false \
DOCKER_COMPOSE_BASE="file://$( abspath ../../../../ )" \
datahub docker quickstart --build-locally --dump-logs-on-failure

echo "--------------------------------------------------------------------"
echo "Setup environment for pytest"
echo "--------------------------------------------------------------------"

./setup_spark_smoke_test.sh

echo "--------------------------------------------------------------------"
echo "Starting pytest"
echo "--------------------------------------------------------------------"

cd ..
#Validate data pushed to the datahub
pytest -vv --continue-on-collection-errors --junit-xml=junit.spark.smoke.xml

