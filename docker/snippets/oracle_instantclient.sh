#!/bin/bash
# Install Oracle Instant Client (Basic package) for glibc-based images. This is the minimum
# zip needed for OCI clients (e.g. Python stack talking to Oracle via the instant client libs).
#
# Bumping versions: set the three values below to match a row on Oracle's Instant Client
# download pages; IC_OTN_ID is the path segment in download.oracle.com/otn_software/.../IC_OTN_ID/
# (e.g. x86-64: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html;
# ARM: https://www.oracle.com/database/technologies/instant-client/linux-arm-aarch64-downloads.html).
# IC_UNZIP_DIR must match the top-level directory inside the Basic ZIP (see unzip -l); Oracle
# uses the same folder name for x64 and arm64 at a given 23.x release.
#
# A stable path /opt/oracle/instantclient -> ${IC_UNZIP_DIR} is created so Python (oracledb
# thick_mode_lib_dir), ORACLE_HOME-style tooling, and user configs do not need updating when
# the Instant Client minor version (and thus the unzip directory name) changes.
#
# libaio: Oracle's loader expects libaio.so.1, but distros differ:
#   - Debian/Ubuntu (multiarch): often only libaio.so.1t64 under /usr/lib/*-linux-gnu/
#   - Wolfi: libaio.so.1 under /usr/lib, while client libs may still look beside multiarch paths
# link_libaio_for_oracle() adds symlinks so resolution succeeds in both cases.
set -euxo pipefail

IC_VERSION=23.26.1.0.0
IC_OTN_ID=2326100
IC_UNZIP_DIR=instantclient_23_26

link_libaio_for_oracle() {
  # Debian/Ubuntu: symlink libaio.so.1t64 -> libaio.so.1 in the multiarch libdir
  for d in /usr/lib/x86_64-linux-gnu /usr/lib/aarch64-linux-gnu; do
    if [ -d "$d" ] && [ -e "$d/libaio.so.1t64" ] && [ ! -e "$d/libaio.so.1" ]; then
      (cd "$d" && ln -sf ./libaio.so.1t64 ./libaio.so.1) || true
    fi
  done
  # Wolfi and similar: system libaio in /usr/lib, Oracle loader may need a link in multiarch dir
  for d in /usr/lib/x86_64-linux-gnu /usr/lib/aarch64-linux-gnu; do
    if [ -d "$d" ] && [ -e /usr/lib/libaio.so.1 ] && [ ! -e "$d/libaio.so.1" ] && [ ! -e "$d/libaio.so.1t64" ]; then
      (cd "$d" && ln -sf ../../lib/libaio.so.1 libaio.so.1) || true
    fi
  done
}

# Download the Basic package for this CPU. Oracle publishes separate zips (x64 vs arm64) with
# the same version string. Other architectures are not supported here.
ic_base_url="https://download.oracle.com/otn_software/linux/instantclient/${IC_OTN_ID}"
mkdir -p /opt/oracle
cd /opt/oracle

if [ "$(arch)" = "x86_64" ]; then
  zip=instantclient-basic-linux.x64-${IC_VERSION}.zip
else
  zip=instantclient-basic-linux.arm64-${IC_VERSION}.zip
fi

wget --no-verbose -c "${ic_base_url}/${zip}"
unzip -q "$zip"
rm -f "$zip"

# Ensure libaio can be found the way Oracle's shared libraries expect (see header).
link_libaio_for_oracle

# Stable path for ldconfig, ORACLE_HOME, thick_mode_lib_dir, ORACLE_CLIENT_LIBRARY_DIR, etc.
ln -sfn "/opt/oracle/${IC_UNZIP_DIR}" /opt/oracle/instantclient

# Register the client directory with the dynamic linker (libclntsh.so, etc.).
sh -c "echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf"
ldconfig
