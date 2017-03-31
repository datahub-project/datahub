#!/bin/bash

VERSION=$1
if [ -z "$VERSION" ]; then
  VERSION=1
fi

docker run -d -p 9001:9000 -v $HOME/logs/wherehows:/home/wherehows/logs wherehows/wherehows:$VERSION
