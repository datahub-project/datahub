#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

set -euo pipefail

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
SPECS_OUT_DIR=$DATAHUB_ROOT/docs/generated/specs

rm -r $SPECS_OUT_DIR || true
python scripts/specgen.py --out-dir ${SPECS_OUT_DIR} $@
