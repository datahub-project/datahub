#!/bin/bash

# Since executor and integrations-service share the Python setup, we can reuse the same scripts.
cp {../datahub-integrations-service/,.}/scripts/lockfile.sh
cp {../datahub-integrations-service/,.}/scripts/lockfile-check.sh
cp {../datahub-integrations-service/,.}/scripts/sync.sh
