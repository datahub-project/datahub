#!/bin/bash

# Since executor and integrations-service share the Python setup, we can reuse the same scripts.
cp {../datahub-integrations-service/,.}/scripts/lockfile.sh
cp {../datahub-integrations-service/,.}/scripts/lockfile-check.sh
cp {../datahub-integrations-service/,.}/scripts/sync.sh

# Remove the duckdb stuff from sync.sh.
sed -i.prev '/# DUCKDB INSTALL START/,/# DUCKDB INSTALL END/d' scripts/sync.sh
rm scripts/sync.sh.prev
