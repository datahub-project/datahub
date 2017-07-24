#!/bin/sh

VERSION=$1
if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  exit 0
fi

# Extract dist zip
rm -rf tmp
unzip ../../wherehows-frontend/build/distributions/wherehows-frontend.zip -d tmp

docker build --force-rm -t linkedin/wherehows-frontend:$VERSION .
docker build --force-rm -t linkedin/wherehows-frontend:latest .

# Clean up
rm -rf tmp