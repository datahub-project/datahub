#!/bin/bash
set -euo pipefail

OUTDIR=./src/datahub/metadata

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..

SCHEMAS_PDL="$DATAHUB_ROOT/metadata-models/src/main/pegasus/com/linkedin"
SCHEMAS_AVSC="$DATAHUB_ROOT/metadata-events/mxe-schemas/src/renamed/avro/com/linkedin"
ENTITY_REGISTRY="$DATAHUB_ROOT/metadata-models/src/main/resources/entity-registry.yml"

rm -r $OUTDIR 2>/dev/null || true
python scripts/avro_codegen.py $ENTITY_REGISTRY $SCHEMAS_PDL $SCHEMAS_AVSC $OUTDIR
