#!/bin/bash
# Wrapper script to run Airflow plugin tests in Docker with automatic volume mounts
# Usage: ./run-tests.sh [tox-env] [-- pytest-args]
#
# Examples:
#   ./run-tests.sh                                    # Run all tests with default env
#   ./run-tests.sh py311-airflow29                    # Run with Airflow 2.9
#   ./run-tests.sh py311-airflow31 -- -k snowflake -v # Run specific tests
#   ./run-tests.sh py311-airflow31 -- --update-golden-files # Update golden files

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the repository root (two directories up from this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo -e "${GREEN}DataHub Airflow Plugin Test Runner${NC}"
echo "Repository root: $REPO_ROOT"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    echo "Please start Docker and try again"
    exit 1
fi

# Build or use existing image
IMAGE_NAME="airflow-plugin-test:latest"
if [[ "${REBUILD:-false}" == "true" ]] || ! docker image inspect "$IMAGE_NAME" > /dev/null 2>&1; then
    echo -e "${YELLOW}Building Docker image...${NC}"
    docker build -f "$SCRIPT_DIR/Dockerfile.test" -t "$IMAGE_NAME" "$REPO_ROOT"
    echo -e "${GREEN}Build complete${NC}"
    echo ""
fi

# Prepare docker run command with automatic volume mounts
DOCKER_CMD=(
    docker run --rm
    # Mount source code (read-write for editable installs)
    -v "$REPO_ROOT/metadata-ingestion:/app/metadata-ingestion"
    # Mount airflow plugin (read-write for golden files and .tox)
    -v "$REPO_ROOT/metadata-ingestion-modules/airflow-plugin:/app/metadata-ingestion-modules/airflow-plugin"
    # Set working directory
    -w /app/metadata-ingestion-modules/airflow-plugin
    # Pass current user ID/GID for Airflow 3.x compatibility
    # (entrypoint will create a user with these IDs)
    -e USER_ID="$(id -u)"
    -e GROUP_ID="$(id -g)"
    # Set HOME to /tmp so all cache directories are writable
    -e HOME=/tmp
    # Set UV cache directory to a writable location
    -e UV_CACHE_DIR=/tmp/.uv-cache
)

# Add image name
DOCKER_CMD+=("$IMAGE_NAME")

# Pass through all arguments
DOCKER_CMD+=("$@")

echo -e "${YELLOW}Running tests...${NC}"
echo "Command: ${DOCKER_CMD[*]}"
echo ""

# Execute the command
exec "${DOCKER_CMD[@]}"