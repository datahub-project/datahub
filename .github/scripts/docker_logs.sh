# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

TARGET_DIR="${TARGET_DIR:=docker_logs}"
TEST_STRATEGY="${TEST_STRATEGY:=}"

mkdir -p "$TARGET_DIR"
for name in `docker ps -a --format '{{.Names}}'`;
do
    docker logs "$name" >& "${TARGET_DIR}/${name}${TEST_STRATEGY}.log" || true
done