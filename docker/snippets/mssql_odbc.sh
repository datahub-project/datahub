#!/bin/bash
# Install Microsoft ODBC Driver 18 for SQL Server.
#
# Wolfi is glibc-based but uses apk. Microsoft's Alpine .apk packages are musl-linked
# and must NOT be used on Wolfi. Install the RHEL 9 glibc RPM instead (--nodeps because
# Wolfi packages are apk-managed and invisible to rpm's dependency solver).
#
# Ubuntu/Debian: use Microsoft's apt repository (glibc .deb packages).
#
# Bumping versions: update MSODBCSQL_VERSION and MSODBCSQL_RELEASE to match a row at
# https://packages.microsoft.com/rhel/9/prod/Packages/m/msodbcsql18-*
set -euxo pipefail

MSODBCSQL_VERSION=18.6.2.1
MSODBCSQL_RELEASE=1
MSODBCSQL_RHEL_REPO="https://packages.microsoft.com/rhel/9/prod/Packages/m"
ODBC_DRIVER_NAME="ODBC Driver 18 for SQL Server"

list_shared_lib_deps() {
  local lib_path="$1"
  if command -v ldd >/dev/null 2>&1; then
    ldd "${lib_path}"
    return 0
  fi

  local loader=""
  case "$(uname -m)" in
    x86_64) loader=/lib/ld-linux-x86-64.so.2 ;;
    aarch64) loader=/lib/ld-linux-aarch64.so.1 ;;
    *)
      echo "Unsupported architecture for shared library verification: $(uname -m)"
      exit 1
      ;;
  esac
  test -x "${loader}" || { echo "missing dynamic linker: ${loader}"; exit 1; }
  "${loader}" --list "${lib_path}"
}

verify_shared_lib() {
  local lib_path="$1"
  local deps=""
  test -n "${lib_path}"
  test -f "${lib_path}" || { echo "missing library: ${lib_path}"; exit 1; }
  deps="$(list_shared_lib_deps "${lib_path}")"
  if echo "${deps}" | grep -q 'not found'; then
    echo "unresolved shared library dependencies for ${lib_path}:"
    echo "${deps}"
    exit 1
  fi
}

verify_driver() {
  odbcinst -j
  driver_so="$(odbcinst -q -d -n "${ODBC_DRIVER_NAME}" | sed -n 's/^Driver=//p')"
  test -n "${driver_so}" || {
    echo "ODBC driver not registered: ${ODBC_DRIVER_NAME}"
    odbcinst -q -d
    exit 1
  }
  verify_shared_lib "${driver_so}"
  odbcinst -q -d -n "${ODBC_DRIVER_NAME}"
}

install_wolfi() {
  case "$(uname -m)" in
    x86_64) arch="x86_64" ;;
    aarch64) arch="aarch64" ;;
    *)
      echo "Unsupported architecture $(uname -m) for msodbcsql18 on Wolfi"
      exit 1
      ;;
  esac

  if ! command -v rpm >/dev/null 2>&1; then
    apk add --no-cache rpm
  fi

  rpm_pkg="msodbcsql18-${MSODBCSQL_VERSION}-${MSODBCSQL_RELEASE}.${arch}.rpm"
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "${tmp_dir}"' EXIT
  cd "${tmp_dir}"

  curl -fsSL -O "${MSODBCSQL_RHEL_REPO}/${rpm_pkg}"

  # EULA auto-accept for driver >= 18.4 (must exist before install).
  mkdir -p /opt/microsoft/msodbcsql18
  touch /opt/microsoft/msodbcsql18/ACCEPT_EULA

  # --nodeps: glibc, unixODBC, krb5, openssl, etc. are already on the image via apk.
  # --nosignature: Microsoft RPM signing key is not in Wolfi's rpm keyring.
  rpm -ivh --nodeps --nosignature "${rpm_pkg}"

  verify_driver
}

install_debian() {
  case "${ID}" in
    ubuntu)
      ms_version="$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)"
      ms_repo="https://packages.microsoft.com/config/ubuntu/${ms_version}/packages-microsoft-prod.deb"
      ;;
    debian)
      ms_version="$(cut -d . -f 1 /etc/debian_version)"
      ms_repo="https://packages.microsoft.com/config/debian/${ms_version}/packages-microsoft-prod.deb"
      ;;
    *)
      echo "Unsupported Debian-family ID=${ID} for mssql_odbc.sh"
      exit 1
      ;;
  esac

  curl -fsSL -o /tmp/packages-microsoft-prod.deb "${ms_repo}"
  dpkg -i /tmp/packages-microsoft-prod.deb
  rm -f /tmp/packages-microsoft-prod.deb

  apt-get update
  ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18
  rm -rf /var/lib/apt/lists/*

  verify_driver
}

. /etc/os-release
case "${ID}" in
  wolfi) install_wolfi ;;
  ubuntu | debian) install_debian ;;
  *)
    echo "Unsupported OS ID=${ID} for mssql_odbc.sh"
    exit 1
    ;;
esac
