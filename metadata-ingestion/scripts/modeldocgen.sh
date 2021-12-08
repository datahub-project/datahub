#!/bin/bash
set -euo pipefail

OUTDIR=./generated/docs

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
REGISTRY_ROOT="$DATAHUB_ROOT/metadata-models/src/main/resources"
SCHEMAS_ROOT="$DATAHUB_ROOT/metadata-models/src/mainGeneratedAvroSchema/avro/com/linkedin"
FILES="$REGISTRY_ROOT/entity-registry.yml $SCHEMAS_ROOT/mxe/MetadataChangeEvent.avsc"
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
        echo $FILES > /tmp/docgen_files.txt
done

FILES=$(cat /tmp/docgen_files.txt)

rm -r $OUTDIR || true
#echo $FILES
python scripts/modeldocgen.py $FILES --dot generated/docs/metadata_graph.dot --file generated/docs/metadata_model_mces.json $@
