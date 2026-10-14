#!/bin/sh
# Unused by docker/datahub-actions/Dockerfile. That image installs
# openssl4_ssl_compat.py as sitecustomize.py in the interpreter stdlib, which
# covers venvs created after this image is built. This script only patches
# venvs that already exist, so a later `uv venv --python` does not see it.
#
# Copy the OpenSSL 4 ssl.PROTOCOL_TLSv1 shim into every venv that can import
# snowflake.connector. A .pth file is required so the shim loads even when a
# subprocess clears PYTHONPATH. Each install uses that venv's Python so
# site.getsitepackages() points at the right site-packages.
set -eu

src="${1:-/tmp/openssl4-ssl-compat}"

install_into() {
    py="$1"
    "$py" -c "import pathlib, shutil, site; dest = pathlib.Path(site.getsitepackages()[0]); src = pathlib.Path('${src}'); shutil.copy(src / 'openssl4_ssl_compat.py', dest / 'openssl4_ssl_compat.py'); shutil.copy(src / 'openssl4-ssl-compat.pth', dest / 'openssl4-ssl-compat.pth')"
}

# Main app venv is first on PATH (VIRTUAL_ENV=/home/datahub/.venv).
install_into python

if [ -d /opt/datahub/venvs ]; then
    for venv in /opt/datahub/venvs/*; do
        if [ -x "$venv/bin/python" ]; then
            install_into "$venv/bin/python"
        fi
    done
fi
