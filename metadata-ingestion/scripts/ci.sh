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

python -m pip install --upgrade pip wheel setuptools
pip install --upgrade apache-airflow==1.10.15 -c https://raw.githubusercontent.com/apache/airflow/constraints-1.10.15/constraints-3.6.txt
airflow db init
python -m pip install -e ".[dev]"

./scripts/codegen.sh

black --check .
isort --check-only .
flake8 --count --statistics .
mypy .
pytest -vv
