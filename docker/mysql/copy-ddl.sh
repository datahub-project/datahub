#!/bin/bash

# Copies over the ddl files

if [ -z "$WORKSPACE" ]; then
  echo "You must set the WORKSPACE environment variable for this to work."
  exit
fi

rm -rf *_DDL
rm bin/create_all_tables_wrapper.sql

DDL_DIR=$WORKSPACE/WhereHows/data-model/DDL
cp $DDL_DIR/create_all_tables_wrapper.sql bin
cp -r $DDL_DIR/*_DDL .

# Unfortunately these scripts may be executed multiple times.
# The data directory is mounted as a volume, meaning that these scripts could run twice for the 
# same directory.  Change schema to just create tables if they do not already exist.
sed -i "" -e "s/CREATE TABLE/CREATE TABLE IF NOT EXISTS/g" *_DDL/*

# In some places we just doubled up on IF NOT EXISTS
sed -i "" -e "s/IF NOT EXISTS IF NOT EXISTS/IF NOT EXISTS/g" *_DDL/*

