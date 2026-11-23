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

# Run the task to regenerate the file.
# `docker compose config` bakes the host environment into the output for every
# variable the profile compose files reference — both `${VAR...}` interpolations
# and bare `VAR:` passthrough entries (e.g. AWS_PROFILE, DATAHUB_LOCAL_*). Unset
# all of them, derived from the compose sources so the list cannot go stale.
# HOME is kept: Gradle needs it, and generateQuickstartComposeConfig substitutes
# it back to ${HOME} in the output.
echo "Running generateQuickstartComposeConfig..."
cd "$REPO_ROOT"

COMPOSE_ENV_VARS=$(
    {
        grep -rhoE '\$\{[A-Za-z_][A-Za-z0-9_]*' docker/profiles/docker-compose*.yml | cut -c3-
        grep -rhoE '^[[:space:]]+[A-Z][A-Z0-9_]*:[[:space:]]*$' docker/profiles/docker-compose*.yml | sed -E 's/^[[:space:]]+//; s/:[[:space:]]*$//'
    } | sort -u | grep -vx 'HOME'
)

UNSET_ARGS=()
for var in $COMPOSE_ENV_VARS; do
    UNSET_ARGS+=(-u "$var")
done

env "${UNSET_ARGS[@]}" \
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
