#!/bin/bash

set -euxo pipefail

# This arg should point at a local copy of https://github.com/hsheth2/sample-dbt,
# after the generation script has been run.
sample_dbt=$1
number=$2

cp $sample_dbt/target_processed/dbt_catalog.json sample_dbt_catalog_$number.json
cp $sample_dbt/target_processed/dbt_manifest.json sample_dbt_manifest_$number.json
cp $sample_dbt/target_processed/dbt_sources.json sample_dbt_sources_$number.json
cp $sample_dbt/target_processed/dbt_run_results.json sample_dbt_run_results_$number.json


