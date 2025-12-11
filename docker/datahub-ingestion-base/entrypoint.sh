#!/usr/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


if [ -n "$ACTIONS_EXTRA_PACKAGES" ]; then
  uv pip install $ACTIONS_EXTRA_PACKAGES
fi

if [[ -n "$ACTIONS_CONFIG" && -n "$ACTIONS_EXTRA_PACKAGES" ]]; then
  mkdir -p /tmp/datahub/logs
  curl -q "$ACTIONS_CONFIG" -o config.yaml
  exec dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s \
    datahub actions --config config.yaml
else
  exec datahub $@
fi
