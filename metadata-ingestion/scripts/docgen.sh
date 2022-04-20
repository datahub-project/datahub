#!/bin/bash
set -euo pipefail

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
DOCS_OUT_DIR=$DATAHUB_ROOT/docs/generated/ingestion
EXTRA_DOCS_DIR=$DATAHUB_ROOT/metadata-ingestion/docs/sources

rm -r $DOCS_OUT_DIR || true
python scripts/docgen.py --out-dir ${DOCS_OUT_DIR} --extra-docs ${EXTRA_DOCS_DIR}
## Full version of this command that generates dot files and png files (requires pydot and graphviz)
# python scripts/modeldocgen.py $FILES --dot generated/docs/metadata_graph.dot --file generated/docs/metadata_model_mces.json --extra-docs ${METADATA_MODEL_DOCS_ROOT} --png generated/docs/metadata_graph.png $@
