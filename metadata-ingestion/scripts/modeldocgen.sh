#!/bin/bash
set -euo pipefail

OUTDIR=./generated/docs
DOCS_OUTDIR=../docs/generated/metamodel

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
SCHEMAS_ROOT="$DATAHUB_ROOT/metadata-events/mxe-schemas/src/mainGeneratedAvroSchema/avro/"
ENTITY_REGISTRY="$DATAHUB_ROOT/metadata-models/src/main/resources/entity-registry.yml"
METADATA_MODEL_DOCS_ROOT="$DATAHUB_ROOT/metadata-models/docs"

rm -r $OUTDIR || true
python scripts/modeldocgen.py $SCHEMAS_ROOT --registry $ENTITY_REGISTRY --generated-docs-dir $DOCS_OUTDIR --file generated/docs/metadata_model_mces.json --extra-docs ${METADATA_MODEL_DOCS_ROOT} $@
## Full version of this command that generates dot files and png files (requires pydot and graphviz)
# python scripts/modeldocgen.py $SCHEMAS_ROOT --registry $ENTITY_REGISTRY --generated-docs-dir $DOCS_OUTDIR --dot generated/docs/metadata_graph.dot --file generated/docs/metadata_model_mces.json --extra-docs ${METADATA_MODEL_DOCS_ROOT} --png generated/docs/metadata_graph.png $@
