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
# Optional fallback hint when GMS system-info is unreachable. Only set postgres when
# the profile clearly indicates it — never force elasticsearch from a stale job-level
# PROFILE_NAME (CI quickstart may use quickstart-postgres while the pytest step still
# inherits the workflow default quickstart-consumers). Prefer live GMS detection in tests.
if [ -z "${DATAHUB_USAGE_EVENTS_IMPLEMENTATION:-}" ]; then
  case "${PROFILE_NAME:-}" in
    *postgres*)
      export DATAHUB_USAGE_EVENTS_IMPLEMENTATION=postgres
      ;;
  esac
fi
