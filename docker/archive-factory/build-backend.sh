#!/bin/bash

if [ -d "tmp" ]; then
  rm -rf tmp
fi

mkdir tmp
cp originals/backend.zip tmp
cd tmp
unzip backend.zip
mv backend-ser* backend-service

# Some people may want to remove the documentation of the API.
# rm -rf backend-service/share
rm backend-service/README.md

# leave the option to change the secret later.
sed -i 's/changeme/$PLAY_CRYPTO_SECRET/g' backend-service/conf/application.conf

tar zcvf backend-service.tar.gz backend-service

cp *.tar.gz ../../backend-service/archives