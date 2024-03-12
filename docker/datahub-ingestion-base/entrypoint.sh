#!/usr/bin/bash

if [ ! -z "$ACTIONS_EXTRA_PACKAGES" ]; then
   pip install --user $ACTIONS_EXTRA_PACKAGES
fi

if [[ ! -z "$ACTIONS_CONFIG" && ! -z "$ACTIONS_EXTRA_PACKAGES" ]]; then
  mkdir -p /tmp/datahub/logs
  curl -q "$ACTIONS_CONFIG" -o config.yaml
  exec dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s \
    datahub actions --config config.yaml
else
  exec datahub $@
fi
