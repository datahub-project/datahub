#!/bin/sh
if [ -f "../static/datahub_json_schema.json" ]; then
  rm ../static/datahub_json_schema.json
fi

echo "Generating Json schema..."
python gen_json_schema.py ../../docs/generated/ingestion/config_schemas/ ../static/datahub_json_schema.json
