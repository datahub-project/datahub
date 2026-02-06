#!/bin/bash
# Manual test script for recording/replay/validation workflow
# 
# Usage:
#   ./test_recording_workflow.sh <recipe.yaml> [password]
#
# Example:
#   ./test_recording_workflow.sh my_snowflake_recipe.yaml test123
#
# This script:
# 1. Records an ingestion run using the provided recipe
# 2. Replays the recording in air-gapped mode
# 3. Validates that recording and replay produce identical MCPs
#
# Requirements:
# - datahub CLI installed in venv
# - Recipe file with valid credentials
# - acryl-datahub[testing-utils] installed for metadata-diff

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <recipe.yaml> [password]"
    echo ""
    echo "Example:"
    echo "  $0 snowflake_recipe.yaml test123"
    exit 1
fi

RECIPE_PATH="$1"
PASSWORD="${2:-test123}"  # Default password: test123

# Validate recipe exists
if [ ! -f "$RECIPE_PATH" ]; then
    echo -e "${RED}Error: Recipe file not found: $RECIPE_PATH${NC}"
    exit 1
fi

# Setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/../../.."
DATAHUB_CLI="$REPO_ROOT/venv/bin/datahub"

# Verify datahub CLI exists
if [ ! -f "$DATAHUB_CLI" ]; then
    echo -e "${RED}Error: datahub CLI not found at $DATAHUB_CLI${NC}"
    echo "Please ensure you're running from the metadata-ingestion directory and venv is set up"
    exit 1
fi

TEMP_DIR=$(mktemp -d)
RECORDING_OUTPUT="$TEMP_DIR/recording_output.json"
REPLAY_OUTPUT="$TEMP_DIR/replay_output.json"
RECORDING_ARCHIVE=""

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  DataHub Recording/Replay Validation Test                   ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Recipe:${NC}     $RECIPE_PATH"
echo -e "${BLUE}Temp dir:${NC}   $TEMP_DIR"
echo -e "${BLUE}Password:${NC}   $(echo $PASSWORD | sed 's/./*/g')"
echo ""

# Cleanup function
cleanup() {
    if [ -n "$RECORDING_ARCHIVE" ] && [ -f "$RECORDING_ARCHIVE" ]; then
        echo -e "\n${BLUE}Recording saved to:${NC} $RECORDING_ARCHIVE"
    fi
    echo -e "\n${BLUE}Temp files in:${NC} $TEMP_DIR"
    echo -e "${YELLOW}To clean up:${NC} rm -rf $TEMP_DIR"
}
trap cleanup EXIT

# Step 1: Recording
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}STEP 1: Recording ingestion run...${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Temporarily modify recipe to output to our temp directory
TEMP_RECIPE="$TEMP_DIR/recipe.yaml"
cat "$RECIPE_PATH" | sed "s|filename:.*|filename: $RECORDING_OUTPUT|" > "$TEMP_RECIPE"

# Run recording
"$DATAHUB_CLI" ingest run -c "$TEMP_RECIPE" \
    --record \
    --record-password "$PASSWORD" \
    --no-s3-upload \
    --no-secret-redaction \
    2>&1 | tee "$TEMP_DIR/recording.log"

# Extract recording archive location from logs
RECORDING_ARCHIVE=$(grep "Created recording archive:" "$TEMP_DIR/recording.log" | tail -1 | awk '{print $NF}')

if [ -z "$RECORDING_ARCHIVE" ] || [ ! -f "$RECORDING_ARCHIVE" ]; then
    echo -e "${RED}✗ Recording failed - no archive created${NC}"
    echo "Check logs: $TEMP_DIR/recording.log"
    exit 1
fi

# Verify recording output exists
if [ ! -f "$RECORDING_OUTPUT" ]; then
    echo -e "${RED}✗ Recording output file not created${NC}"
    exit 1
fi

# Count MCPs by parsing JSON array (more accurate than wc -l)
RECORDING_MCP_COUNT=$(python3 -c "import json; f=open('$RECORDING_OUTPUT'); data=json.load(f); f.close(); print(len(data) if isinstance(data, list) else 1 if isinstance(data, dict) else 0)")
echo ""
echo -e "${GREEN}✓ Recording complete${NC}"
echo -e "  Archive: $RECORDING_ARCHIVE"
echo -e "  MCPs:    $RECORDING_MCP_COUNT"

# Step 2: Replay
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}STEP 2: Replaying in air-gapped mode...${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo ""

"$DATAHUB_CLI" ingest replay "$RECORDING_ARCHIVE" \
    --password "$PASSWORD" \
    2>&1 | tee "$TEMP_DIR/replay.log"

# Find replay output file
REPLAY_FILE=$(ls -t /tmp/datahub_replay_*.json 2>/dev/null | head -1)
if [ -z "$REPLAY_FILE" ] || [ ! -f "$REPLAY_FILE" ]; then
    echo -e "${RED}✗ Replay failed - no output file created${NC}"
    echo "Check logs: $TEMP_DIR/replay.log"
    exit 1
fi

cp "$REPLAY_FILE" "$REPLAY_OUTPUT"
# Count MCPs by parsing JSON array (more accurate than wc -l)
REPLAY_MCP_COUNT=$(python3 -c "import json; f=open('$REPLAY_OUTPUT'); data=json.load(f); f.close(); print(len(data) if isinstance(data, list) else 1 if isinstance(data, dict) else 0)")

echo ""
echo -e "${GREEN}✓ Replay complete${NC}"
echo -e "  Output:  $REPLAY_OUTPUT"
echo -e "  MCPs:    $REPLAY_MCP_COUNT"

# Step 3: Validation
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}STEP 3: Validating MCP semantic equivalence...${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Check MCP counts (warning only, as duplicates can cause differences)
if [ "$RECORDING_MCP_COUNT" != "$REPLAY_MCP_COUNT" ]; then
    echo -e "${YELLOW}⚠ MCP count mismatch (this may be due to duplicates)${NC}"
    echo "  Recording: $RECORDING_MCP_COUNT MCPs"
    echo "  Replay:    $REPLAY_MCP_COUNT MCPs"
    echo ""
fi

# Run metadata-diff
echo "Comparing MCPs (ignoring timestamps)..."
if "$DATAHUB_CLI" check metadata-diff \
    --ignore-path "root['*']['systemMetadata']['lastObserved']" \
    --ignore-path "root['*']['systemMetadata']['runId']" \
    --ignore-path "root['*']['auditStamp']['time']" \
    --ignore-path "root['*']['aspect']['json']['created']['time']" \
    --ignore-path "root['*']['aspect']['json']['lastModified']['time']" \
    --ignore-path "root\[\\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\\d+\]\['auditStamp'\]\['time'\]" \
    "$RECORDING_OUTPUT" \
    "$REPLAY_OUTPUT" \
    > "$TEMP_DIR/diff_output.txt" 2>&1; then
    
    echo -e "${GREEN}✓ Metadata diff passed (exit code 0)${NC}"
else
    echo -e "${RED}✗ Metadata diff failed${NC}"
    cat "$TEMP_DIR/diff_output.txt"
    echo ""
    echo -e "${YELLOW}To re-run the diff manually:${NC}"
    echo "$DATAHUB_CLI check metadata-diff \\"
    echo "    --ignore-path \"root['*']['systemMetadata']['lastObserved']\" \\"
    echo "    --ignore-path \"root['*']['systemMetadata']['runId']\" \\"
    echo "    --ignore-path \"root['*']['auditStamp']['time']\" \\"
    echo "    --ignore-path \"root['*']['aspect']['json']['created']['time']\" \\"
    echo "    --ignore-path \"root['*']['aspect']['json']['lastModified']['time']\" \\"
    echo "    --ignore-path \"root\[\\\\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\\\\d+\]\['auditStamp'\]\['time'\]\" \\"
    echo "    \"$RECORDING_OUTPUT\" \\"
    echo "    \"$REPLAY_OUTPUT\""
    exit 1
fi

# Final summary
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  ✅ ALL VALIDATIONS PASSED!                                  ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Summary:${NC}"
echo -e "  Recording:  $RECORDING_MCP_COUNT MCPs"
echo -e "  Replay:     $REPLAY_MCP_COUNT MCPs"
echo -e "  Match:      PERFECT (metadata-diff exit code 0)"
echo ""
echo -e "${BLUE}Files:${NC}"
echo -e "  Recording output:  $RECORDING_OUTPUT"
echo -e "  Replay output:     $REPLAY_OUTPUT"
echo -e "  Recording archive: $RECORDING_ARCHIVE"
echo -e "  Logs:              $TEMP_DIR/*.log"
echo ""
echo -e "${GREEN}🎉 Recording/Replay workflow validated successfully!${NC}"

