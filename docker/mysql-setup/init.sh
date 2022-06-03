#!/bin/bash

: ${MYSQL_PORT:=3306}

sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" \
 /init.sql | tee /tmp/init-final.sql

if [[ $CREATE_USER == true ]]; then
  sed -e "s/DATAHUB_DB_NAME/${DATAHUB_DB_NAME}/g" \
    -e "s/USER_NAME/${MYSQL_USERNAME}/g" \
    -e "s/PASSWORD/${MYSQL_PASSWORD}/g" \
   /user.sql | tee /tmp/user-final.sql
  mysql -u $MYSQL_ROOT_USERNAME -p"$MYSQL_ROOT_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT < /tmp/init-final.sql
  mysql -u $MYSQL_ROOT_USERNAME -p"$MYSQL_ROOT_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT < /tmp/user-final.sql
else
  mysql -u $MYSQL_USERNAME -p"$MYSQL_PASSWORD" -h $MYSQL_HOST -P $MYSQL_PORT < /tmp/init-final.sql
fi
