# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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