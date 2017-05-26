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

tar zcvf backend-service.tar.gz backend-service

cp *.tar.gz ../../backend-service/archives