#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


set -euxo pipefail

# This arg should point at a local copy of https://github.com/acryldata/sample-dbt,
# after the generation script has been run.
sample_dbt=$1
number=$2

cp $sample_dbt/target_processed/dbt_catalog.json sample_dbt_catalog_$number.json
cp $sample_dbt/target_processed/dbt_manifest.json sample_dbt_manifest_$number.json
cp $sample_dbt/target_processed/dbt_sources.json sample_dbt_sources_$number.json
cp $sample_dbt/target_processed/dbt_run_results.json sample_dbt_run_results_$number.json


