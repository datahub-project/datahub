#!/bin/bash
set -euxo pipefail

# This script will use the YML files in examples/demo_data to generate
# all_covid19_datasets.json, directives.csv, and finally demo_data.json.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Fetch public COVID-19 datasets from BigQuery.
datahub ingest -c "$DIR/bigquery_covid19_to_file.yml"

# Pull the directives CSV from Google sheets.
# See https://docs.google.com/spreadsheets/d/17c5SBiXEw5PuV7oEkC2uQnX55C6TPZTnr6XRQ6X-Qy0/edit#gid=0.
DIRECTIVES_URL="https://docs.google.com/spreadsheets/d/e/2PACX-1vSUtBW2wEb3AO0fk8XsZRauVzdFpXb3Jj_G3L3ngmNPUsnB-12KW_JRXIqpXpZYeYMuaiQQrM8Huu3f/pub?gid=0&single=true&output=csv"
curl -sS -L "${DIRECTIVES_URL}" --output $DIR/directives.csv

# Enrich the COVID-19 datasets using the directives.
python $DIR/enrich.py
