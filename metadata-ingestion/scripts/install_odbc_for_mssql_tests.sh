#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


UBUNTU_VERSION=$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)


if ! [[ "20.04 22.04 24.04 24.10" == *"$UBUNTU_VERSION"* ]];
then
    echo "Ubuntu $UBUNTU_VERSION is not currently supported.";
    exit;
fi

sudo su

# Download the package to configure the Microsoft repo
curl -sSL -O https://packages.microsoft.com/config/ubuntu/$UBUNTU_VERSION/packages-microsoft-prod.deb
# Install the package
sudo dpkg -i packages-microsoft-prod.deb
# Delete the file
rm packages-microsoft-prod.deb

# Install the driver
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18