#!/bin/bash -xe
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

#Remove old configuration
rm -rf workspace

#Copy needed files 
mkdir workspace

ls ../../

cp ../../build/libs/datahub-spark-lineage* workspace/
cp ../spark-docker.conf workspace/
cp -a ../python-spark-lineage-test workspace/
mkdir workspace/java-spark-lineage-test
cp ../test-spark-lineage/java_test_run.sh workspace/java-spark-lineage-test/

mkdir -p workspace/java-spark-lineage-test/build/libs/
cp ../test-spark-lineage/build/libs/test-spark-lineage.jar workspace/java-spark-lineage-test/build/libs/

cp -a ../resources workspace

# create docker images
docker build -f SparkBase.Dockerfile -t spark-base .
docker build -f SparkMaster.Dockerfile -t spark-master .
docker build -f SparkSlave.Dockerfile -t spark-slave .
docker build -f SparkSubmit.Dockerfile -t spark-submit .

