#!/bin/bash
set -euo pipefail

# Run the observability dashboard using isolated experimental venv

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"
VENV_DIR="$PROJECT_DIR/.venv-experimental"
DASHBOARD="$SCRIPT_DIR/../observability_dashboard.py"

# Check if venv exists
if [ ! -d "$VENV_DIR" ]; then
    echo "❌ Experimental venv not found. Setting up..."
    "$SCRIPT_DIR/setup_experimental_venv.sh"
fi

# Activate venv and run dashboard
echo "🚀 Starting observability dashboard..."
echo "   Dashboard: http://localhost:8501"
echo ""
echo "   Press Ctrl+C to stop"
echo ""

cd "$PROJECT_DIR"
source "$VENV_DIR/bin/activate"
exec streamlit run "$DASHBOARD"
