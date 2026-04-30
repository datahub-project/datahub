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
# libaio: Oracle's loader expects libaio.so.1, but distros differ:
#   - Debian/Ubuntu (multiarch): often only libaio.so.1t64 under /usr/lib/*-linux-gnu/
#   - Wolfi: libaio.so.1 under /usr/lib, while client libs may still look beside multiarch paths
# link_libaio_for_oracle() adds symlinks so resolution succeeds in both cases.
#
# RUNPATH: Oracle ships libclntsh.so without a RUNPATH, so its DT_NEEDED deps
# (libnnz*, libclntshcore, libons, ...) can only be located via LD_LIBRARY_PATH
# or /etc/ld.so.cache. That breaks any caller that points at this directory
# from a process whose loader env isn't set up — e.g. python-oracledb's
# init_oracle_client(lib_dir=...) on a stock Linux box. patch_oracle_runpath()
# embeds RUNPATH=$ORIGIN into the client libs so the loader looks in their own
# directory first. We still ldconfig the dir below as defense in depth.
# See https://github.com/oracle/python-oracledb/issues/578.
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

# Make patchelf available regardless of which base image we're on. Returns
# non-zero if no supported package manager is found, so the caller can decide
# whether to skip RUNPATH patching.
ensure_patchelf() {
  if command -v patchelf >/dev/null 2>&1; then return 0; fi
  if command -v apt-get >/dev/null 2>&1; then
    DEBIAN_FRONTEND=noninteractive apt-get update -qq \
      && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends patchelf
  elif command -v apk >/dev/null 2>&1; then
    apk add --no-cache patchelf
  else
    return 1
  fi
}

# Embed RUNPATH=$ORIGIN into every .so* in the IC directory so the dynamic
# linker can resolve their DT_NEEDED deps from the directory itself, without
# any external configuration. See header comment for context.
patch_oracle_runpath() {
  if ! ensure_patchelf; then
    echo "patchelf unavailable; skipping RUNPATH=\$ORIGIN patch (ldconfig fallback still applies)" >&2
    return 0
  fi
  # Patch real files only; the compatibility symlinks Oracle ships
  # (e.g. libclntsh.so -> libclntsh.so.23.1) transparently inherit the new
  # RUNPATH from their target.
  find "/opt/oracle/${IC_UNZIP_DIR}" -maxdepth 1 -type f -name '*.so*' -print0 \
    | while IFS= read -r -d '' so; do
        # shellcheck disable=SC2016
        # $ORIGIN is a dynamic-linker token, not a shell variable; pass it
        # through literally for patchelf to embed in the DT_RUNPATH.
        patchelf --set-rpath '$ORIGIN' "$so" 2>/dev/null \
          || echo "patchelf failed on $so (continuing)" >&2
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

# Make the client libs self-locating so they work regardless of LD_LIBRARY_PATH/ldconfig.
patch_oracle_runpath

# Register the client directory with the dynamic linker (libclntsh.so, etc.).
sh -c "echo /opt/oracle/${IC_UNZIP_DIR} > /etc/ld.so.conf.d/oracle-instantclient.conf"
ldconfig
