#!/usr/bin/bash

if [ -n "$ACTIONS_EXTRA_PACKAGES" ]; then
  uv pip install $ACTIONS_EXTRA_PACKAGES
fi

if [[ -n "$ACTIONS_CONFIG" && -n "$ACTIONS_EXTRA_PACKAGES" ]]; then
  mkdir -p /tmp/datahub/logs
  curl -q "$ACTIONS_CONFIG" -o config.yaml
  source /usr/local/lib/datahub/wait_for_deps.sh
  datahub_wait_begin
  datahub_wait_http "${DATAHUB_GMS_PROTOCOL:-http}://${DATAHUB_GMS_HOST}:${DATAHUB_GMS_PORT}/health" ""
  exec datahub actions --config config.yaml
else
  exec datahub $@
fi
