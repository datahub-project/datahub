#!/bin/bash
# Start the FastAPI server

echo "Starting DataHub Chat API server..."
echo "Documentation: http://localhost:8000/api/docs"
echo ""

# Change to backend directory
cd "$(dirname "$0")"

# Run uvicorn with reload
uvicorn api.main:app --reload --port 8000 --host 0.0.0.0
