#!/bin/bash
set -euxo pipefail

sudo apt-get update && sudo apt-get install -y \
    librdkafka-dev \
    python3-ldap \
    libldap2-dev \
    libsasl2-dev \
    ldap-utils

python -m pip install --upgrade pip wheel setuptools
pip install -e ".[dev]"

./scripts/codegen.sh

black --check src tests
isort --check-only src tests
flake8 --count --statistics src tests
mypy -p datahub

pytest
