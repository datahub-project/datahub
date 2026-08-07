#!/bin/bash
# Self-contained script to build bundled venvs for DataHub ingestion sources.
# Named groups share one physical venv ({label}-venv); member plugins may be
# symlinked to <plugin-name>-bundled for executor compatibility.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration from environment variables
BUNDLED_VENV_PLUGINS="${BUNDLED_VENV_PLUGINS:-s3,demo-data,file}"
BUNDLED_CLI_VERSION="${BUNDLED_CLI_VERSION:-}"
DATAHUB_BUNDLED_VENV_PATH="${DATAHUB_BUNDLED_VENV_PATH:-/opt/datahub/venvs}"

# Prefer explicit BUNDLED_CLI_VERSION (inherited from the base image ENV when
# extending datahub-actions / datahub-executor). If unset, fall back to the
# acryl-datahub version already installed in the image.
if [ -z "$BUNDLED_CLI_VERSION" ]; then
    BUNDLED_CLI_VERSION="$(
        python -c 'import importlib.metadata as m; print(m.version("acryl-datahub"))' 2>/dev/null || true
    )"
    if [ -n "$BUNDLED_CLI_VERSION" ]; then
        echo "BUNDLED_CLI_VERSION unset; using installed acryl-datahub version: $BUNDLED_CLI_VERSION"
        export BUNDLED_CLI_VERSION
    fi
fi

if [ -z "$BUNDLED_CLI_VERSION" ]; then
    echo "ERROR: BUNDLED_CLI_VERSION is unset and acryl-datahub is not installed in this image"
    echo "Set BUNDLED_CLI_VERSION explicitly, or build FROM an image that ships it / acryl-datahub."
    exit 1
fi

echo "=============================================="
echo "Building bundled venvs for DataHub ingestion"
echo "=============================================="
echo "DataHub CLI Version: $BUNDLED_CLI_VERSION"
echo "Plugins: $BUNDLED_VENV_PLUGINS"
echo "Group env vars: set BUNDLED_VENV_PLUGINS_<group> (e.g. BUNDLED_VENV_PLUGINS_COMMON); optional."
echo "Venv Path: $DATAHUB_BUNDLED_VENV_PATH"
echo ""

# Ensure the venv directory exists
mkdir -p "$DATAHUB_BUNDLED_VENV_PATH"

# Use the self-contained Python script to generate and create venvs (same directory as this script)
echo "Running bundled venv builder..."
cd "$SCRIPT_DIR"
python ./build_bundled_venvs_unified.py

# Verify the venvs were created
echo ""
echo "=============================================="
echo "Verification"
echo "=============================================="
echo "Bundled venvs created in $DATAHUB_BUNDLED_VENV_PATH:"
ls -la "$DATAHUB_BUNDLED_VENV_PATH/"
echo ""
echo "Symlinks (sample):"
find "$DATAHUB_BUNDLED_VENV_PATH" -maxdepth 1 -type l 2>/dev/null | head -20 | while read -r linkpath; do
  echo "  $linkpath -> $(readlink "$linkpath")"
done
echo ""
echo "Total entries: $(ls -1 "$DATAHUB_BUNDLED_VENV_PATH/" | wc -l)"

# Ensure bundled venvs are owned by the runtime user when built as root.
if [ "$(id -u)" -eq 0 ] && id -u datahub >/dev/null 2>&1; then
    echo "Setting ownership of $DATAHUB_BUNDLED_VENV_PATH to datahub:datahub"
    chown -R datahub:datahub "$DATAHUB_BUNDLED_VENV_PATH"
fi

echo "Bundled venv build completed successfully!"
