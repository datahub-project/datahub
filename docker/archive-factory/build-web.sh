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

tar zcvf web.tar.gz wherehows

cp web.tar.gz ../../web/archives