#!/bin/bash

set -euxo pipefail

echo 'Installing dependencies based on lockfile.'
uv pip sync requirements.txt requirements-dev.txt
