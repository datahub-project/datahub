#!/bin/bash
set -euo pipefail

# Run the chat UI using isolated experimental venv

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"
VENV_DIR="$PROJECT_DIR/.venv-experimental"
CHAT_UI="$SCRIPT_DIR/../chat_ui.py"

# Check if venv exists
if [ ! -d "$VENV_DIR" ]; then
    echo "❌ Experimental venv not found. Setting up..."
    "$SCRIPT_DIR/setup_experimental_venv.sh"
fi

# Activate venv and run chat UI
echo "🚀 Starting chat UI..."
echo "   Chat UI: http://localhost:8502"
echo ""
echo "   Press Ctrl+C to stop"
echo ""

cd "$PROJECT_DIR"
source "$VENV_DIR/bin/activate"
exec streamlit run "$CHAT_UI" --server.port 8502
