#!/bin/bash

VERSION=$1
if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  exit 0
fi

# Build a single init SQL script from multiple DDLs.
echo 'use wherehows;' > init.sql
cat ../../wherehows-data-model/DDL/ETL_DDL/*.sql >> init.sql
cat ../../wherehows-data-model/DDL/WEB_DDL/*.sql >> init.sql

docker build --force-rm -t linkedin/wherehows-mysql:$VERSION .
docker build --force-rm -t linkedin/wherehows-mysql:latest .