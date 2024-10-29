#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

set -euxo pipefail

python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip wheel setuptools
pip install -r requirements.txt
python generate_docker_quickstart.py check-all
