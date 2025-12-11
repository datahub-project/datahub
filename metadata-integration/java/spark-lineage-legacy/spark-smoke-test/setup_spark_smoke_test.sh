#!/bin/bash -x
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


set -e

SMOKE_TEST_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

pip install -r requirements.txt

echo "--------------------------------------------------------------------"
echo "Building java test framework"
echo "--------------------------------------------------------------------"


cd test-spark-lineage
./gradlew build
cd ..

echo "--------------------------------------------------------------------"
echo "Building spark images"
echo "--------------------------------------------------------------------"

cd docker

#build spark cluster images
./build_images.sh 

echo "--------------------------------------------------------------------"
echo "Bringing up spark cluster"
echo "--------------------------------------------------------------------"

cd "${SMOKE_TEST_ROOT_DIR}"/docker
#bring up spark cluster
docker compose -f spark-docker-compose.yml up -d

echo "--------------------------------------------------------------------"
echo "Executing spark-submit jobs"
echo "--------------------------------------------------------------------"

#Execute spark-submit jobs
docker run --network datahub_network spark-submit

echo "--------------------------------------------------------------------"
echo "Starting pytest"
echo "--------------------------------------------------------------------"

