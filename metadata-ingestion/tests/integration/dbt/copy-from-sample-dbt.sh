#!/bin/bash

set -euxo pipefail

# This arg should point at a local copy of https://github.com/hsheth2/sample-dbt,
# after the generation script has been run.
sample_dbt=$1

cp $sample_dbt/target_processed/dbt_catalog.json sample_dbt_catalog.json
cp $sample_dbt/target_processed/dbt_manifest.json sample_dbt_manifest.json
cp $sample_dbt/target_processed/dbt_sources.json sample_dbt_sources.json

# We don't currently test run_results from sample-dbt.
# cp $sample_dbt/target_processed/dbt_run_results.json sample_dbt_run_results.json


