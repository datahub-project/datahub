#!/bin/sh
export PGPASSWORD=$POSTGRES_PASSWORD

POSTGRES_CREATE_DB=${POSTGRES_CREATE_DB:-true}
POSTGRES_CREATE_DB_CONNECTION_DB=${POSTGRES_CREATE_DB_CONNECTION_DB:-postgres}

# workaround create database if not exists, check https://stackoverflow.com/a/36591842
if [ "$POSTGRES_CREATE_DB" = true ]; then
    psql -d "$POSTGRES_CREATE_DB_CONNECTION_DB" -U "$POSTGRES_USERNAME" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -tc "SELECT 1 FROM pg_database WHERE datname = '${DATAHUB_DB_NAME}'" | grep -q 1 || psql -d "$POSTGRES_CREATE_DB_CONNECTION_DB" -U "$POSTGRES_USERNAME" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -c "CREATE DATABASE ${DATAHUB_DB_NAME}"
fi

sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" /init.sql | tee -a /tmp/init-final.sql
psql -d "$DATAHUB_DB_NAME" -U "$POSTGRES_USERNAME" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" < /tmp/init-final.sql
