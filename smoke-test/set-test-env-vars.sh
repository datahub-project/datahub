# Handle base path properly to avoid double slashes
if [ "${DATAHUB_BASE_PATH}" = "/" ] || [ -z "${DATAHUB_BASE_PATH}" ]; then
    export DATAHUB_KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8080/schema-registry/api
else
    export DATAHUB_KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8080${DATAHUB_BASE_PATH}/schema-registry/api
fi
# Handle base path properly to avoid double slashes
if [ "${DATAHUB_BASE_PATH}" = "/" ] || [ -z "${DATAHUB_BASE_PATH}" ]; then
    export DATAHUB_GMS_URL=http://localhost:8080
else
    export DATAHUB_GMS_URL=http://localhost:8080${DATAHUB_BASE_PATH}
fi