#!/bin/bash
set -euo pipefail

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
SPECS_OUT_DIR=$DATAHUB_ROOT/docs/generated/specs

rm -r $SPECS_OUT_DIR || true
python scripts/specgen.py --out-dir ${SPECS_OUT_DIR} $@
