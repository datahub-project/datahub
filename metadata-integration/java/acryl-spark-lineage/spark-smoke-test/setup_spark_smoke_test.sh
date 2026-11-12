#!/bin/bash -x

set -e

SMOKE_TEST_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

pip install -r requirements.txt

echo "--------------------------------------------------------------------"
echo "Building java test framework"
echo "--------------------------------------------------------------------"


cd test-spark-lineage
# This nested build uses Gradle 8.14.3, which does not support JDK 25 (its Groovy can't read
# class-file v69 -> "Unsupported class file major version 69" during _BuildScript_ compilation).
# It's a Spark 3.x app that must build+run on Java 17/21 anyway, so build it with JDK 21 (exported by
# the workflow's setup-java as JAVA_HOME_21_X64); fall back to the ambient JAVA_HOME for local runs.
JAVA_HOME="${JAVA_HOME_21_X64:-${JAVA_HOME_21_ARM64:-$JAVA_HOME}}" ./gradlew build
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
docker run \
  -e SPARK_DRIVER_EXTRA_JAVA_OPTIONS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  --network datahub_network \
  spark-submit

echo "--------------------------------------------------------------------"
echo "Starting pytest"
echo "--------------------------------------------------------------------"

