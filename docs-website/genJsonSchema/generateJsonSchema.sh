#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

SCHEMA_FILE_NAME=datahub_ingestion_schema.json
SCHEMA_ROOT_DIR=../static/schemas
if [ -f "${SCHEMA_ROOT_DIR}/${SCHEMA_FILE_NAME}" ]; then
  rm ${SCHEMA_ROOT_DIR}/${SCHEMA_FILE_NAME}
fi

echo "Generating Json schema..."
python3 gen_json_schema.py ../../docs/generated/ingestion/config_schemas/ ${SCHEMA_ROOT_DIR}/${SCHEMA_FILE_NAME}
