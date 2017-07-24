#!/bin/sh

VERSION=$1
if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  exit 0
fi

# Extract dist zip
rm -rf tmp
unzip ../../wherehows-backend/build/distributions/wherehows-backend.zip -d tmp

docker build --force-rm -t linkedin/wherehows-backend:$VERSION .
docker build --force-rm -t linkedin/wherehows-backend:latest .

# Clean up
rm -rf tmp