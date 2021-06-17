#!/bin/bash
set -euxo pipefail

OUTDIR=./src/datahub/metadata

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
SCHEMAS_ROOT="$DATAHUB_ROOT/metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe"
FILES="$SCHEMAS_ROOT/MetadataChangeEvent.avsc $SCHEMAS_ROOT/MetadataAuditEvent.avsc"

rm -r $OUTDIR || true
python scripts/avro_codegen.py $FILES $OUTDIR
