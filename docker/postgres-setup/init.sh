#!/bin/sh
export PGPASSWORD=$POSTGRES_PASSWORD

sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" /init.sql | tee -a /tmp/init-final.sql
psql -d $POSTGRES_DATABASENAME -U $POSTGRES_USERNAME -h $POSTGRES_HOST -p $POSTGRES_PORT < /tmp/init-final.sql
