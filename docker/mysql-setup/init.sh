#!/bin/bash

: ${MYSQL_PORT:=3306}
: ${MYSQL_ARGS:=--ssl=0}

sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" /init.sql | tee -a /tmp/init-final.sql
mariadb -u $MYSQL_USERNAME -p"$MYSQL_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT $MYSQL_ARGS < /tmp/init-final.sql