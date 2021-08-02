#!/bin/bash
set -euxo pipefail

OUTDIR=./src/datahub/metadata

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
SCHEMAS_ROOT="$DATAHUB_ROOT/metadata-events/mxe-schemas/src/renamed/avro/com/linkedin"
FILES="$SCHEMAS_ROOT/mxe/MetadataChangeEvent.avsc $SCHEMAS_ROOT/mxe/MetadataChangeProposal.avsc $SCHEMAS_ROOT/usage/UsageAggregation.avsc $SCHEMAS_ROOT/dataset/DatasetProfile.avsc"

rm -r $OUTDIR || true
python scripts/avro_codegen.py $FILES $OUTDIR
