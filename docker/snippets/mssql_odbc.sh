#!/bin/bash

set -eux

. /etc/os-release

case $ID in
  ubuntu)
    UBUNTU_VERSION=$(lsb_release -rs)
    curl -sSL https://packages.microsoft.com/config/ubuntu/$UBUNTU_VERSION/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
    ;;

  debian)
    DEBIAN_VERSION=$(cat /etc/debian_version | cut -d . -f 1)
    curl -sSL https://packages.microsoft.com/config/debian/$DEBIAN_VERSION/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
    ;;

  *) echo "This is an unknown distribution."
    ;;
esac

curl -sSL https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
gpg -o /usr/share/keyrings/microsoft-prod.gpg --dearmor /etc/apt/trusted.gpg.d/microsoft.asc

apt-get update
ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18
rm -rf /var/lib/apt/lists/*

odbcinst -j || { echo 'odbcinst -j failed'; exit 1; }
odbcinst -q -d -n "ODBC Driver 18 for SQL Server" || { echo 'ODBC Driver 18 for SQL Server not found'; exit 1; }
