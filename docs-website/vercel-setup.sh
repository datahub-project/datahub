#!/bin/bash

set -euxo pipefail

./metadata-ingestion/scripts/install_deps.sh

# Set up java version for gradle
yum install java-17-amazon-corretto-devel -y
javac --version

# Build python from source.
# Amazon Linux 2 has Python 3.8, but it's version of OpenSSL is super old and hence it
# doesn't work with the packages we use. As such, we have to build Python from source.
# TODO: This process is extremely slow - ideally we should cache the built Python binary
# for reuse.

yum groupinstall "Development Tools" -y
yum install openssl openssl-devel libffi-devel bzip2-devel wget nodejs -y

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
python3 --version

