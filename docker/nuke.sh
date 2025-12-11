#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

# Tear down and clean up all DataHub-related containers, volumes, and network
docker compose -p datahub down -v
docker compose rm -f -v

# Tear down ingestion container
(cd ingestion && docker compose -p datahub down -v)
