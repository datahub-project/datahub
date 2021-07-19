#!/bin/bash
set -euxo pipefail

# We allow for failures in this step. Usually you'll be invoking this
# script to fix a build failure.
pytest "$@" --update-golden-files || true

# Print success message.
set +x
echo ''
echo 'Make sure to check `git diff -w` to verify the changes!'
