#!/bin/bash
set -euxo pipefail

(cd .. && ./gradlew :metadata-events:mxe-schemas:build)
./scripts/ci.sh

rm -rf build dist || true
python -m build
python -m twine upload 'dist/*'
