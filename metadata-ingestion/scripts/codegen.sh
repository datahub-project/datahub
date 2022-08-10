#!/bin/bash
set -euo pipefail

OUTDIR=./src/datahub/metadata

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
SCHEMAS_ROOT="$DATAHUB_ROOT/metadata-events/mxe-schemas/src/renamed/avro/com/linkedin"

rm -r $OUTDIR 2>/dev/null || true
python scripts/avro_codegen.py $SCHEMAS_ROOT $OUTDIR
