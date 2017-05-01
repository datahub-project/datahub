#!/bin/bash

if [ -z "$MYSQL_ROOT_PASSWORD" ]; then
  echo "You must set MYSQL_ROOT_PASSWORD"
  exit
fi

ARG="$@"
if [ -z "$ARG" ]; then
  echo "No default argument was provided, using mysqld"
  ARG='mysqld'
fi

./usr/local/bin/async-init.sh &

# run the normal start up.
# note that mysqld is the default argument, provided by the original docker image.
./entrypoint.sh $ARG | tee /var/log/mysql/entrypoint.log
