#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


# read -p "Desired datahub version, for example 'v0.10.1' (defaults to newest): " VERSION
VERSION=${DATAHUB_VERSION:-head}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
IMAGE=acryldata/datahub-upgrade:$DATAHUB_VERSION
cd $DIR && docker pull ${IMAGE} && docker run --env-file ./env/docker.env --network="datahub_network" ${IMAGE} "$@"
