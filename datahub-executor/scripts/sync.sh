#!/bin/bash

set -euxo pipefail

echo 'Installing dependencies based on lockfile.'
uv pip sync requirements.txt requirements-dev.txt

# TODO: If in CI, throw an error if the requirement files aren't in sync with
# the package dependencies.
