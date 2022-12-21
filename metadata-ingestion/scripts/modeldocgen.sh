#!/bin/bash
set -euo pipefail

OUTDIR=./generated/docs

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
REGISTRY_ROOT="$DATAHUB_ROOT/metadata-models/src/main/resources"
SCHEMAS_ROOT="$DATAHUB_ROOT/metadata-events/mxe-schemas/src/mainGeneratedAvroSchema/avro/"
FILES="$REGISTRY_ROOT/entity-registry.yml $SCHEMAS_ROOT/com/linkedin/mxe/MetadataChangeEvent.avsc"
METADATA_MODEL_DOCS_ROOT="$DATAHUB_ROOT/metadata-models/docs"
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
python scripts/modeldocgen.py $FILES --file generated/docs/metadata_model_mces.json --extra-docs ${METADATA_MODEL_DOCS_ROOT} $@
## Full version of this command that generates dot files and png files (requires pydot and graphviz)
# python scripts/modeldocgen.py $FILES --dot generated/docs/metadata_graph.dot --file generated/docs/metadata_model_mces.json --extra-docs ${METADATA_MODEL_DOCS_ROOT} --png generated/docs/metadata_graph.png $@
