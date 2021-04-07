#!/bin/bash
set -euxo pipefail

if [ "$(uname)" == "Darwin" ]; then
    brew install librdkafka
else
    sudo apt-get update && sudo apt-get install -y \
        librdkafka-dev \
        python3-ldap \
        libldap2-dev \
        libsasl2-dev \
        ldap-utils
fi

python -m pip install --upgrade pip==20.2.4 wheel setuptools
pip install -e ".[dev]"

./scripts/codegen.sh

black --check .
isort --check-only .
flake8 --count --statistics .
mypy .
pytest -vv
