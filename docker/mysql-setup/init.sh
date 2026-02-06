#!/bin/bash
set -euo pipefail

: ${MYSQL_PORT:=3306}
: ${MYSQL_ARGS:=--ssl=0}
: ${CDC_MCL_PROCESSING_ENABLED:=false}
: ${CDC_USER:=datahub_cdc}
: ${CDC_PASSWORD:=datahub_cdc}

# Process main init script
sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" /init.sql | tee -a /tmp/init-final.sql

# Run main init script with regular user
mariadb -u $MYSQL_USERNAME -p"$MYSQL_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT $MYSQL_ARGS < /tmp/init-final.sql

# Conditionally create CDC user with root privileges if enabled
if [[ "${CDC_MCL_PROCESSING_ENABLED}" == "true" ]]; then
    echo "CDC MCL processing is enabled. Creating CDC user with root privileges..."

    if [[ -z "${MYSQL_ROOT_PASSWORD}" ]]; then
        echo "ERROR: MYSQL_ROOT_PASSWORD must be set when CDC_MCL_PROCESSING_ENABLED=true"
        exit 1
    fi

    # Create CDC user script with root privileges
    sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" \
        -e "s/CDC_USER/${CDC_USER}/g" \
        -e "s/CDC_PASSWORD/${CDC_PASSWORD}/g" /init-cdc.sql > /tmp/init-cdc-final.sql

    # Execute CDC user creation with root user
    mariadb -u root -p"$MYSQL_ROOT_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT $MYSQL_ARGS < /tmp/init-cdc-final.sql

    echo "CDC user '$CDC_USER' created successfully."
fi