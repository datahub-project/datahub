#!/bin/bash
# Setup script for local development with Google ADK examples

set -e

echo "Setting up Google ADK examples for local development..."
echo ""

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "Error: Please run this script from examples/google_adk directory"
    exit 1
fi

# Check for Python
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

echo "Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "Setup complete!"
echo ""
echo "To run the examples, set the following environment variables:"
echo ""
echo "  export GOOGLE_API_KEY='your-api-key-here'      # Gemini Developer API"
echo "  export DATAHUB_GMS_URL='http://localhost:8080'  # Optional, this is the default"
echo "  export DATAHUB_GMS_TOKEN='your-token'           # Optional"
echo ""
echo "Then run any example:"
echo "  python simple_search.py"
echo "  python basic_agent.py"
echo ""
echo "To use Vertex AI instead of the Gemini Developer API, set up Application Default"
echo "Credentials (ADC) and omit GOOGLE_API_KEY:"
echo "  gcloud auth application-default login"
echo ""
