#!/bin/bash

set -e
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

#bring up spark cluster
docker-compose -f spark-docker-compose.yml up -d

echo "--------------------------------------------------------------------"
echo "Executing spark-submit jobs"
echo "--------------------------------------------------------------------"

#Execute spark-submit jobs
docker run --network datahub_network spark-submit

echo "--------------------------------------------------------------------"
echo "Starting pytest"
echo "--------------------------------------------------------------------"

