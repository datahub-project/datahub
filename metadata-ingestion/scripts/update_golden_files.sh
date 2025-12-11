#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

set -euxo pipefail

# We allow for failures in this step. Usually you'll be invoking this
# script to fix a build failure.
pytest "$@" --update-golden-files || true

# Print success message.
set +x
echo ''
echo 'Make sure to check `git diff -w` to verify the changes!'
