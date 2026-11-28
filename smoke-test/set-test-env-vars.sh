# The Docker-free stack (SMOKE_BACKEND=local) exports its own URLs from
# `datahub-dev.sh shell-env --tests`; its ports depend on the stack's slot.
if [ "${SMOKE_BACKEND:-docker}" = "local" ] && [ -n "${DATAHUB_GMS_URL:-}" ]; then
    return 0
fi

# Set default base path if not provided
DATAHUB_GMS_BASE_PATH=${DATAHUB_GMS_BASE_PATH:-}

# Handle base path properly to avoid double slashes
if [ "${DATAHUB_GMS_BASE_PATH}" = "/" ] || [ -z "${DATAHUB_GMS_BASE_PATH}" ]; then
    export DATAHUB_KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8080/schema-registry/api
else
    export DATAHUB_KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8080${DATAHUB_GMS_BASE_PATH}/schema-registry/api
fi
# Handle base path properly to avoid double slashes
if [ "${DATAHUB_GMS_BASE_PATH}" = "/" ] || [ -z "${DATAHUB_GMS_BASE_PATH}" ]; then
    export DATAHUB_GMS_URL=http://localhost:8080
else
    export DATAHUB_GMS_URL=http://localhost:8080${DATAHUB_GMS_BASE_PATH}
fi