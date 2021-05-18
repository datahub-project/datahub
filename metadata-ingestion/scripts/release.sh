#!/bin/bash
set -euxo pipefail

../gradlew build  # also runs tests

rm -rf build dist || true
python -m build
python -m twine upload 'dist/*'
