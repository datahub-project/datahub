#!/bin/bash

# Runs a basic e2e test. It is not meant to be fully comprehensive,
# but rather should catch obvious bugs before they make it into prod.
#
# Script assumptions:
#   - The gradle build has already been run.
#   - Python 3.6+ is installed.
#   - The metadata-ingestion codegen script has been run.
#   - A full DataHub setup is running on localhost with standard ports.
#     The easiest way to do this is by using the quickstart or dev
#     quickstart scripts.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

set -euxo pipefail

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt

(cd ../metadata-ingestion && ./scripts/codegen.sh)

pytest -vv
