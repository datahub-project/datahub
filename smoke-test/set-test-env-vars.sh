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
# Align smoke-test process env with docker-compose.gms.yml defaults for the active
# profile. Postgres primary-datastore profiles default usage-events SoT to postgres.
if [ -z "${DATAHUB_USAGE_EVENTS_IMPLEMENTATION:-}" ]; then
  case "${PROFILE_NAME:-}" in
    *postgres*)
      export DATAHUB_USAGE_EVENTS_IMPLEMENTATION=postgres
      ;;
    *)
      export DATAHUB_USAGE_EVENTS_IMPLEMENTATION=elasticsearch
      ;;
  esac
fi
