#!/bin/bash

: ${MYSQL_PORT:=3306}
: ${MYSQL_ARGS:=--ssl=0}

sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" \
 /init.sql | tee /tmp/init-final.sql

if [[ $CREATE_USER == true ]]; then
  sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" \
    -e "s/USER_NAME/${MYSQL_USERNAME}/g" \
    -e "s/PASSWORD/${MYSQL_PASSWORD}/g" \
   /user.sql | tee /tmp/user-final.sql
  mariadb -u $MYSQL_ROOT_USERNAME -p"$MYSQL_ROOT_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT $MYSQL_ARGS < /tmp/init-final.sql
  mariadb -u $MYSQL_ROOT_USERNAME -p"$MYSQL_ROOT_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT $MYSQL_ARGS < /tmp/user-final.sql
else
  mariadb -u $MYSQL_USERNAME -p"$MYSQL_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT $MYSQL_ARGS < /tmp/init-final.sql
fi
