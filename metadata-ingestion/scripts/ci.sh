#!/bin/bash
set -euxo pipefail

sudo apt-get update && sudo apt-get install -y librdkafka-dev python-ldap libldap2-dev libsasl2-dev ldap-utils

python -m pip install --upgrade pip
pip install -e .
pip install -r test_requirements.txt

./scripts/codegen.sh

black --check --exclude 'datahub/metadata' -S -t py36 src tests
isort --check-only src tests
flake8 --count --statistics src tests
mypy -p datahub

pytest
