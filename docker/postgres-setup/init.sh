#!/bin/sh
export PGPASSWORD=$POSTGRES_PASSWORD

# workaround create database if not exists, check https://stackoverflow.com/a/36591842
psql -U $POSTGRES_USERNAME -h $POSTGRES_HOST -p $POSTGRES_PORT -tc "SELECT 1 FROM pg_database WHERE datname = '${DATAHUB_DB_NAME}'" | grep -q 1 || psql -U $POSTGRES_USERNAME -h $POSTGRES_HOST -p $POSTGRES_PORT -c "CREATE DATABASE ${DATAHUB_DB_NAME}"

sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" /init.sql | tee -a /tmp/init-final.sql
psql -d $DATAHUB_DB_NAME -U $POSTGRES_USERNAME -h $POSTGRES_HOST -p $POSTGRES_PORT < /tmp/init-final.sql
