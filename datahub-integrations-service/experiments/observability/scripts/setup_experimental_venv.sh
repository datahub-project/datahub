#!/bin/bash
set -euo pipefail

# Setup isolated venv for experimental observability tools (dashboard + chat UI)
# This keeps experimental dependencies separate from production

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"
VENV_DIR="$PROJECT_DIR/.venv-experimental"
REQUIREMENTS="$SCRIPT_DIR/../requirements.txt"

echo "Setting up experimental venv at: $VENV_DIR"

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating new venv..."
    uv venv "$VENV_DIR"
else
    echo "Using existing venv..."
fi

# Activate and install dependencies
echo "Installing dependencies from $REQUIREMENTS..."
source "$VENV_DIR/bin/activate"
uv pip install -r "$REQUIREMENTS"

# Install datahub-integrations in editable mode
echo "Installing datahub-integrations in editable mode..."
uv pip install -e "$PROJECT_DIR"

echo ""
echo "✅ Experimental venv ready!"
echo ""
echo "To activate manually:"
echo "  source $VENV_DIR/bin/activate"
echo ""
echo "To run dashboard:"
echo "  ./experiments/observability/scripts/run_dashboard.sh"
echo ""
echo "To run chat UI:"
echo "  ./experiments/observability/scripts/run_chat_ui.sh"
