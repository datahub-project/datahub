#!/bin/bash
# usage: ./run_ingest.sh <recipe-id> <datahub-version> <plugins-required> <tmp-dir>

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

source $VENV_NAME/bin/activate

if (python3 -m datahub ingest -c "$4/$1.yml"); then
  exit 0
else
  exit 1
fi
