#!/bin/bash -x
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


set -e
# Script assumptions:
#   - Python 3.6+ is installed and in the PATH.
#   - pytest is installed
#   - requests is installed

is_healthy() {
    local service="$1"
    local -r -i max_attempts="$2"; shift
    local -i attempt_num=1

    until [ -n "$(docker ps -f name="$service" -f "health=healthy"|tail -n +2)" ]
    do
        if (( attempt_num == max_attempts ))
        then
            echo "Attempt $attempt_num failed and there are no more attempts left!"
            return 1
        else
            echo "Attempt $attempt_num failed! Trying again in $attempt_num seconds..."
            sleep $(( attempt_num++ ))
        fi
    done
}

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

if [ "${RUN_QUICKSTART:-true}" == "true" ]; then
  DATAHUB_TELEMETRY_ENABLED=false  \
  DOCKER_COMPOSE_BASE="file://$(cd "$(dirname "${DIR}/../../../../../")"; pwd)" \
  datahub docker quickstart --dump-logs-on-failure
fi
is_healthy "datahub-gms" 60

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

