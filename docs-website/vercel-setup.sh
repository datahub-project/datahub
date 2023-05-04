#!/bin/bash

set -euxo pipefail

./metadata-ingestion/scripts/install_deps.sh

# Build python from source.

yum groupinstall "Development Tools" -y
yum erase openssl-devel -y
yum install openssl11 openssl11-devel  libffi-devel bzip2-devel wget -y

wget https://www.python.org/ftp/python/3.10.11/Python-3.10.11.tgz
tar -xf Python-3.10.11.tgz
cd Python-3.10.11
./configure #--enable-optimizations

make -j $(nproc)

make install

# Set python3.10 as the default version.
py3="$(which python3)"
rm "$py3"
ln "$(which python3.10)" "$py3"

