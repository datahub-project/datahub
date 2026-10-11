#!/usr/bin/env bash
# Check if the quickstart docker-compose config is up-to-date
# This script runs generateQuickstartComposeConfig and fails if it modifies the output file

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_FILE="$REPO_ROOT/docker/quickstart/docker-compose.quickstart-profile.yml"

# Save the original file content
if [ -f "$OUTPUT_FILE" ]; then
    ORIGINAL_CONTENT=$(cat "$OUTPUT_FILE")
else
    ORIGINAL_CONTENT=""
fi

# Run the task to regenerate the file
# Unset all DATAHUB_LOCAL_* environment variables to ensure a clean, reproducible build
echo "Running generateQuickstartComposeConfig..."
cd "$REPO_ROOT"
env -u DATAHUB_LOCAL_ACTIONS_ENV \
    -u DATAHUB_LOCAL_COMMON_ENV \
    -u DATAHUB_LOCAL_FRONTEND_ENV \
    -u DATAHUB_LOCAL_GMS_ENV \
    -u DATAHUB_LOCAL_MAE_ENV \
    -u DATAHUB_LOCAL_MCE_ENV \
    -u DATAHUB_LOCAL_SYS_UPDATE_ENV \
    ./gradlew :docker:generateQuickstartComposeConfig -x generateGitPropertiesGlobal --quiet

# Check if the file content changed
if [ -f "$OUTPUT_FILE" ]; then
    NEW_CONTENT=$(cat "$OUTPUT_FILE")
else
    NEW_CONTENT=""
fi

if [ "$ORIGINAL_CONTENT" != "$NEW_CONTENT" ]; then
    echo ""
    echo "ERROR: docker-compose.quickstart-profile.yml is out of date!"
    echo ""
    echo "The file docker/quickstart/docker-compose.quickstart-profile.yml has been modified"
    echo "by running generateQuickstartComposeConfig. This file must be committed along with"
    echo "your other changes."
    echo ""
    echo "To fix this:"
    echo "  1. Review the changes to docker/quickstart/docker-compose.quickstart-profile.yml"
    echo "  2. Stage the file: git add docker/quickstart/docker-compose.quickstart-profile.yml"
    echo "  3. Commit again"
    echo ""
    exit 1
fi

echo "quickstart docker-compose config is up-to-date"
exit 0
