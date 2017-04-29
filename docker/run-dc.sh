#!/bin/bash


source .env

if [ -z "$1" ]; then
  rm -rf $EXTERNAL_DATA_DIR/mysql
  rm -rf $EXTERNAL_LOGS_DIR/mysql
  rm -rf $EXTERNAL_LOGS_DIR/wherehows
fi

docker-compose up
