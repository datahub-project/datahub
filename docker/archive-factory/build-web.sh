#!/bin/bash

if [ -d "tmp" ]; then
  rm -rf tmp
fi

mkdir tmp
cp originals/web.zip tmp
cd tmp
unzip web.zip
mv wherehows-* wherehows

# some may wish to remove the docs
# rm -rf wherehows/share

rm README.md

# correct a few things that won't work in docker.

# leave the option to change the play crypto secret later.
sed -i 's/changeme/$PLAY_CRYPTO_SECRET/g' wherehows/conf/application.conf

# localhost usually does not work within docker, give people the power
# to change that from outside of docker with environment variables.
sed -i 's/localhost:8888/$HDFS_HOST:8888/g' wherehows/conf/application.conf
sed -i 's/localhost/$MYSQL_HOST/g' wherehows/conf/application.conf

tar zcvf web.tar.gz wherehows

cp web.tar.gz ../../web/archives