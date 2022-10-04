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

datahub docker quickstart \
	--build-locally \
	--quickstart-compose-file ../../../../docker/docker-compose.yml \
	--quickstart-compose-file ../../../../docker/docker-compose.override.yml \
	--quickstart-compose-file ../../../../docker/docker-compose.dev.yml \
	--dump-logs-on-failure

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

