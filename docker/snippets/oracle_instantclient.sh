#!/bin/bash
set -euxo pipefail

if [ $(arch) = "x86_64" ]; then
    mkdir /opt/oracle
    cd /opt/oracle
    wget --no-verbose -c https://download.oracle.com/otn_software/linux/instantclient/2115000/instantclient-basic-linux.x64-21.15.0.0.0dbru.zip
    unzip instantclient-basic-linux.x64-21.15.0.0.0dbru.zip
    rm instantclient-basic-linux.x64-21.15.0.0.0dbru.zip
    sh -c "echo /opt/oracle/instantclient_21_15 > /etc/ld.so.conf.d/oracle-instantclient.conf"
    ldconfig
else
    mkdir /opt/oracle
    cd /opt/oracle
    wget --no-verbose -c https://download.oracle.com/otn_software/linux/instantclient/1923000/instantclient-basic-linux.arm64-19.23.0.0.0dbru.zip
    unzip instantclient-basic-linux.arm64-19.23.0.0.0dbru.zip
    rm instantclient-basic-linux.arm64-19.23.0.0.0dbru.zip
    sh -c "echo /opt/oracle/instantclient_19_23 > /etc/ld.so.conf.d/oracle-instantclient.conf"
    ldconfig
fi
