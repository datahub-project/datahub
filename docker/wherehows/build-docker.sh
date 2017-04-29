#!/bin/bash

VERSION=$1
if [ -z "$VERSION" ]; then
  VERSION=1
fi

docker build -t wherehows/wherehows:$VERSION .
