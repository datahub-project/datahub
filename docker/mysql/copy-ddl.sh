#!/bin/bash

# Copies over the ddl files

# Get parent directory of this file.
# e.g. /Users/me/workspace/WhereHows/docker/mysql
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

rm -rf *_DDL
rm -f bin/create_all_tables_wrapper.sql

DDL_DIR=${SCRIPT_DIR}/../../data-model/DDL
mkdir -p bin
cp $DDL_DIR/create_all_tables_wrapper.sql bin
cp -r $DDL_DIR/*_DDL .

# Unfortunately these scripts may be executed multiple times.
# The data directory is mounted as a volume, meaning that these scripts could run twice for the 
# same directory.  Change schema to just create tables if they do not already exist.
sed -i "" -e "s/CREATE TABLE/CREATE TABLE IF NOT EXISTS/g" *_DDL/*

# In some places we just doubled up on IF NOT EXISTS
sed -i "" -e "s/IF NOT EXISTS IF NOT EXISTS/IF NOT EXISTS/g" *_DDL/*

