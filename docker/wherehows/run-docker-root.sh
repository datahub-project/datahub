#!/bin/bash

VERSION=$1
if [ -z "$VERSION" ]; then
  VERSION=1
fi

docker run -it -u root -v $HOME/logs/wherehows:/home/wherehows/logs wherehows/wherehows:$VERSION /bin/bash
