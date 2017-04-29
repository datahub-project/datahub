#!/bin/bash

VERSION=$1
if [ -z "$VERSION" ]; then
  VERSION=1
fi

docker run -it -p 9000:9000 -p 9001:9008 -v $HOME/logs/wherehows:/home/wherehows/logs wherehows/wherehows:$VERSION /bin/bash
