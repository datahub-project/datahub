#!/bin/bash
set -euxo pipefail

if [ "$(uname)" == "Darwin" ]; then
    brew install librdkafka
else
    sudo_cmd=""
    if command -v sudo; then
        sudo_cmd="sudo"
    fi

    if command -v yum; then
        $sudo_cmd yum install -y \
            librdkafka-devel \
            openldap-devel \
            cyrus-sasl-devel \
            openldap-clients
    else
        $sudo_cmd apt-get update && $sudo_cmd apt-get install -y \
            librdkafka-dev \
            python3-ldap \
            libldap2-dev \
            libsasl2-dev \
            ldap-utils
    fi
fi
