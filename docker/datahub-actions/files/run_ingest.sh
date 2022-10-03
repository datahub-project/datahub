#!/bin/bash
# usage: ./run_ingest.sh <task-id> <datahub-version> <plugins-required> <tmp-dir> <recipe_file> <report_file>

set -euo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit

task_id="$1"
datahub_version="$2"
plugins="$3"
tmp_dir="$4"
recipe_file="$5"
report_file="$6"
debug_mode="$7"

source $VENV_NAME/bin/activate

if (datahub ingest run --help | grep -q report-to); then
  echo "This version of datahub supports report-to functionality"
  rm -f "$report_file"
  report_option="--report-to ${report_file}"
else
  report_option=""
fi

if [ "$debug_mode" == "true" ]; then
  debug_option="--debug"
else
  debug_option=""
fi;

echo "Running ingestion with arguments $@"

# Execute DataHub recipe, based on the recipe id.
echo "datahub ${debug_option} ingest run -c ${recipe_file} ${report_option}"
if (datahub ${debug_option} ingest run -c "${recipe_file}" ${report_option}); then
  exit 0
else
  exit 1
fi