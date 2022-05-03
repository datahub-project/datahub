#!/bin/bash
set -euo pipefail

OUTDIR=./src/datahub/metadata

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
SCHEMAS_ROOT="$DATAHUB_ROOT/metadata-events/mxe-schemas/src/renamed/avro/com/linkedin"
FILES="$SCHEMAS_ROOT/mxe/MetadataChangeEvent.avsc $SCHEMAS_ROOT/mxe/MetadataChangeProposal.avsc $SCHEMAS_ROOT/usage/UsageAggregation.avsc $SCHEMAS_ROOT/mxe/MetadataChangeLog.avsc $SCHEMAS_ROOT/mxe/PlatformEvent.avsc $SCHEMAS_ROOT/platform/event/v1/EntityChangeEvent.avsc"
# Since we depend on jq, check if jq is installed
if ! which jq > /dev/null; then
   echo "jq is not installed. Please install jq and rerun (https://stedolan.github.io/jq/)"
   exit 1
fi

find $SCHEMAS_ROOT -name "*.avsc" | sort | while read file
do
# Add all other files that are aspects but not included in the above
        if (jq '.Aspect' -e $file > /dev/null)
        then
            FILES="${FILES} ${file}"
        fi
        echo $FILES > /tmp/codegen_files.txt
done

FILES=$(cat /tmp/codegen_files.txt)

rm -r $OUTDIR || true
python scripts/avro_codegen.py $FILES $OUTDIR
