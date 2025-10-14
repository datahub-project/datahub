#!/bin/bash
# Self-contained script to build bundled venvs for DataHub ingestion sources
# This script creates venvs with predictable names: <plugin-name>-bundled
# which are then leveraged within acryl-executor to run ingestion jobs.
set -euo pipefail

# Configuration from environment variables
BUNDLED_VENV_PLUGINS="${BUNDLED_VENV_PLUGINS:-s3,demo-data}"
BUNDLED_CLI_VERSION="${BUNDLED_CLI_VERSION:-}"
DATAHUB_BUNDLED_VENV_PATH="${DATAHUB_BUNDLED_VENV_PATH:-/opt/datahub/venvs}"

# Validate required environment variables
if [ -z "$BUNDLED_CLI_VERSION" ]; then
    echo "ERROR: BUNDLED_CLI_VERSION environment variable must be set"
    exit 1
fi

echo "=============================================="
echo "Building bundled venvs for DataHub ingestion"
echo "=============================================="
echo "DataHub CLI Version: $BUNDLED_CLI_VERSION"
echo "Plugins: $BUNDLED_VENV_PLUGINS"
echo "Venv Path: $DATAHUB_BUNDLED_VENV_PATH"
echo ""

# Ensure the venv directory exists
mkdir -p "$DATAHUB_BUNDLED_VENV_PATH"

# Use the self-contained Python script to generate and create venvs
echo "Running bundled venv builder..."

# Determine the Python helper location from common candidates
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_HELPER_CANDIDATES=(
  "${SCRIPT_DIR}/build_bundled_venvs_unified.py"
  "/usr/local/bin/build_bundled_venvs_unified.py"
  "./build_bundled_venvs_unified.py"
)

PY_HELPER=""
for cand in "${PY_HELPER_CANDIDATES[@]}"; do
  if [ -f "$cand" ]; then
    PY_HELPER="$cand"
    break
  fi
done

if [ -z "$PY_HELPER" ]; then
  echo "ERROR: Could not locate build_bundled_venvs_unified.py in any known location."
  echo "Checked: ${PY_HELPER_CANDIDATES[*]}"
  exit 2
fi

echo "Using Python helper: $PY_HELPER"
python "$PY_HELPER"

# Verify the venvs were created
echo ""
echo "=============================================="
echo "Verification"
echo "=============================================="
echo "Bundled venvs created in $DATAHUB_BUNDLED_VENV_PATH:"
ls -la "$DATAHUB_BUNDLED_VENV_PATH/"
echo ""
echo "Total venvs: $(ls -1 "$DATAHUB_BUNDLED_VENV_PATH/" | wc -l)"

echo "Bundled venv build completed successfully!"