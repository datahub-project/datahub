#!/bin/bash

VERSION=$1
if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  exit 0
fi

cp ../../wherehows-data-model/ELASTICSEARCH/index_mapping.json index_mapping.json

docker build --force-rm -t linkedin/wherehows-elasticsearch:$VERSION .
docker build --force-rm -t linkedin/wherehows-elasticsearch:latest .

# Clean up
rm -rf index_mapping.json