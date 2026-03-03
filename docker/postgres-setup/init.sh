#!/bin/sh
set -euo pipefail

export PGPASSWORD=$POSTGRES_PASSWORD

POSTGRES_CREATE_DB=${POSTGRES_CREATE_DB:-true}
POSTGRES_CREATE_DB_CONNECTION_DB=${POSTGRES_CREATE_DB_CONNECTION_DB:-postgres}
CDC_MCL_PROCESSING_ENABLED=${CDC_MCL_PROCESSING_ENABLED:-false}
CDC_USER=${CDC_USER:-datahub_cdc}
CDC_PASSWORD=${CDC_PASSWORD:-datahub_cdc}

# workaround create database if not exists, check https://stackoverflow.com/a/36591842
if [ "$POSTGRES_CREATE_DB" = true ]; then
    psql -d "$POSTGRES_CREATE_DB_CONNECTION_DB" -U "$POSTGRES_USERNAME" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -tc "SELECT 1 FROM pg_database WHERE datname = '${DATAHUB_DB_NAME}'" | grep -q 1 || psql -d "$POSTGRES_CREATE_DB_CONNECTION_DB" -U "$POSTGRES_USERNAME" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -c "CREATE DATABASE ${DATAHUB_DB_NAME}"
fi

# Process main init script
sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" /init.sql | tee -a /tmp/init-final.sql

# Run main init script with regular user
psql -d "$DATAHUB_DB_NAME" -U "$POSTGRES_USERNAME" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" < /tmp/init-final.sql

# Conditionally create CDC user if enabled
if [ "${CDC_MCL_PROCESSING_ENABLED}" = "true" ]; then
    echo "CDC MCL processing is enabled. Creating CDC user..."

    if [ -z "${POSTGRES_PASSWORD}" ]; then
        echo "ERROR: POSTGRES_PASSWORD must be set when CDC_MCL_PROCESSING_ENABLED=true"
        exit 1
    fi

    # Create CDC user script
    sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" \
        -e "s/CDC_USER/${CDC_USER}/g" \
        -e "s/CDC_PASSWORD/${CDC_PASSWORD}/g" /init-cdc.sql > /tmp/init-cdc-final.sql

    # Execute CDC user creation with regular user (who has sufficient privileges)
    psql -d "$POSTGRES_CREATE_DB_CONNECTION_DB" -U "$POSTGRES_USERNAME" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" < /tmp/init-cdc-final.sql

    echo "CDC user '$CDC_USER' created successfully."
fi
