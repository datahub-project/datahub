#!/bin/sh

sed -i -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" /init.sql
mysql -u $MYSQL_USERNAME -p"$MYSQL_PASSWORD" -h $MYSQL_HOST < /init.sql