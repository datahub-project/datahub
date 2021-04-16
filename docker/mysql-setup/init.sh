#!/bin/sh

sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" \
  -e "s/USER_NAME/${DATAHUB_DB_NAME}/g" \
  -e "s/PASSWORD/${MYSQL_PASSWORD}/g" \
 /init.sql | tee /tmp/init-final.sql
mysql -u $MYSQL_ROOT_USERNAME -p"$MYSQL_ROOT_PASSWORD" -h $MYSQL_HOST < /tmp/init-final.sql
