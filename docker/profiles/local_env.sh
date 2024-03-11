#!/bin/bash

# This creates a few empty environment files which
# can be used to allow developer/environment specific
# overrides for the defaults

DATAHUB_DOCKER_HOME="${HOME}/.datahub/docker"
DATAHUB_COMPONENTS="common gms mae mce frontend system-update"

for COMPONENT in $DATAHUB_COMPONENTS
do
  mkdir -p "$DATAHUB_DOCKER_HOME/$COMPONENT"
  touch "$DATAHUB_DOCKER_HOME/$COMPONENT/env.local"
done
