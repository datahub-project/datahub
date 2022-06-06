#!/bin/sh
SCHEMA_FILE_NAME=datahub_ingestion_schema.json
SCHEMA_ROOT_DIR=../static/schemas
if [ -f "${SCHEMA_ROOT_DIR}/${SCHEMA_FILE_NAME}" ]; then
  rm ${SCHEMA_ROOT_DIR}/${SCHEMA_FILE_NAME}
fi

echo "Generating Json schema..."
python gen_json_schema.py ../../docs/generated/ingestion/config_schemas/ ${SCHEMA_ROOT_DIR}/${SCHEMA_FILE_NAME}
