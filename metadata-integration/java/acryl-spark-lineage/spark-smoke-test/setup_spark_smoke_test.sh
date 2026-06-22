#!/bin/bash -x

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

# The Glue job uses the file emitter; bind-mount a host dir so its MCP output is readable by pytest.
mkdir -p "${SMOKE_TEST_ROOT_DIR}/glue-output"

#Execute spark-submit jobs
docker run \
  -e SPARK_DRIVER_EXTRA_JAVA_OPTIONS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED" \
  -e AWS_ACCESS_KEY_ID=test \
  -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_REGION=us-east-1 \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e AWS_ENDPOINT_URL=http://moto:5000 \
  -e AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED \
  -e AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED \
  -v "${SMOKE_TEST_ROOT_DIR}/glue-output":/opt/workspace/glue-output \
  --network datahub_network \
  spark-submit

echo "--------------------------------------------------------------------"
echo "Starting pytest"
echo "--------------------------------------------------------------------"

