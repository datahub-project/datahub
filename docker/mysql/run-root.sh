#!/bin/bash

VERSION=$1
if [ -z "$VERSION" ]; then
  VERSION=1
fi

docker run -it -e MYSQL_ROOT_PASSWORD=wherehows -p 3306:3306 --entrypoint /bin/bash wherehows/mysql-wherehows:$VERSION
