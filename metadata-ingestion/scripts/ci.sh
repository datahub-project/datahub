#!/bin/bash
set -euxo pipefail

python -m pip install --upgrade pip
pip install -e .
pip install -r test_requirements.txt

./scripts/codegen.sh

black --check --exclude 'gometa/metadata' -S -t py36 src tests
isort --check-only src tests
flake8 --count --statistics src tests
mypy -p gometa

pytest
