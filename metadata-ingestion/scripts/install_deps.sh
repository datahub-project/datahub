#!/bin/bash
set -euxo pipefail

if [ "$(uname)" == "Darwin" ]; then
    brew install librdkafka
else
    sudo apt-get update && sudo apt-get install -y \
        librdkafka-dev \
        python3-ldap \
        libldap2-dev \
        libsasl2-dev \
        ldap-utils
fi
