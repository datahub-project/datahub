#!/bin/bash
set -euxo pipefail

OUTDIR=./src/datahub/metadata

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
cp $DATAHUB_ROOT/metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc .

rm -r $OUTDIR || true
python scripts/avro_codegen.py MetadataChangeEvent.avsc $OUTDIR
rm MetadataChangeEvent.avsc
