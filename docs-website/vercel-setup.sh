#!/bin/bash

set -euxo pipefail

./metadata-ingestion/scripts/install_deps.sh

# Set up java version for gradle
yum install java-17-amazon-corretto-devel -y
javac --version

yum groupinstall "Development Tools" -y
yum install openssl openssl-devel libffi-devel bzip2-devel wget nodejs tar -y

# Amazon Linux 2 has Python 3.8, but it's version of OpenSSL is super old and hence it
# doesn't work with the packages we use. As such, we want to install Python ourselves.
# Using uv to install Python from https://github.com/indygreg/python-build-standalone
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

uv python install 3.10

# Set python3.10 as the default version.
py3="$(which python3)"
rm "$py3"
ln "$(uv python find 3.10)" "$py3"
python3 --version

