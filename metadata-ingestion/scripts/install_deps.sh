#!/bin/bash
set -euxo pipefail

if [ "$(uname)" == "Darwin" ]; then
    # None
    true
else
    sudo_cmd=""
    if command -v sudo; then
        sudo_cmd="sudo"
    fi

    if command -v yum; then
        $sudo_cmd yum install -y \
            openldap-devel \
            cyrus-sasl-devel \
            openldap-clients \
            sqlite-devel \
            xz-devel \
            libxml2-devel \
            libxslt-devel \
            krb5-devel
    elif command -v apk; then
        $sudo_cmd apk add \
            build-base \
            openldap-dev \
            xz-dev \
            krb5-dev
    else
        $sudo_cmd apt-get update && $sudo_cmd apt-get install -y \
            python3-ldap \
            libldap2-dev \
            libsasl2-dev \
            ldap-utils \
            libkrb5-dev
    fi
fi
