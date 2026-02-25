#!/bin/bash
#
# Download test artifacts from recent successful CI runs.
#
# This script uses the GitHub CLI (gh) to fetch workflow runs and download
# test result artifacts, organizing them by run ID for later processing.

set -euo pipefail

# Default values
OUTPUT_DIR="./dev-artifacts/test-results"
RUN_COUNT=3
WORKFLOW_NAME="docker-unified.yml"
REPOSITORY=""

# Parse arguments
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Download test artifacts from recent successful CI runs.

OPTIONS:
    --output-dir DIR     Output directory for artifacts (default: ./dev-artifacts/test-results)
    --run-count N        Number of recent runs to download (default: 3)
    --workflow NAME      Workflow file name (default: docker-unified.yml)
    --repository REPO    Repository in format owner/repo (default: auto-detect from git)
    -h, --help           Show this help message

REQUIREMENTS:
    - GitHub CLI (gh) must be installed and authenticated
    - Must be run from within a git repository (unless --repository is specified)

EXAMPLE:
    $0 --output-dir ./dev-artifacts/test-results --run-count 3
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --run-count)
            RUN_COUNT="$2"
            shift 2
            ;;
        --workflow)
            WORKFLOW_NAME="$2"
            shift 2
            ;;
        --repository)
            REPOSITORY="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Check for gh CLI
if ! command -v gh &> /dev/null; then
    echo "Error: GitHub CLI (gh) is not installed."
    echo "Install from: https://cli.github.com/"
    exit 1
fi

# Check gh authentication
if ! gh auth status &> /dev/null; then
    echo "Error: GitHub CLI is not authenticated."
    echo "Run: gh auth login"
    exit 1
fi

# Auto-detect repository if not specified
if [[ -z "$REPOSITORY" ]]; then
    if ! git rev-parse --git-dir &> /dev/null; then
        echo "Error: Not in a git repository and --repository not specified"
        exit 1
    fi

    # Get remote URL and extract owner/repo
    REMOTE_URL=$(git config --get remote.origin.url || echo "")
    if [[ -z "$REMOTE_URL" ]]; then
        echo "Error: No remote.origin.url found in git config"
        exit 1
    fi

    # Parse repository from various URL formats
    if [[ $REMOTE_URL =~ github.com[:/]([^/]+)/([^/.]+) ]]; then
        REPOSITORY="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    else
        echo "Error: Could not parse repository from remote URL: $REMOTE_URL"
        exit 1
    fi
fi

echo "============================================================"
echo "Downloading test artifacts"
echo "============================================================"
echo "Repository: $REPOSITORY"
echo "Workflow: $WORKFLOW_NAME"
echo "Run count: $RUN_COUNT"
echo "Output directory: $OUTPUT_DIR"
echo "============================================================"
echo

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Fetch recent successful workflow runs from master branch
echo "Fetching recent successful workflow runs from master branch..."
RUN_IDS=$(gh api "repos/$REPOSITORY/actions/workflows/$WORKFLOW_NAME/runs" \
    --jq ".workflow_runs[] | select(.conclusion==\"success\" and .head_branch==\"master\") | .id" \
    | head -n "$RUN_COUNT")

if [[ -z "$RUN_IDS" ]]; then
    echo "Error: No successful workflow runs found on master branch"
    exit 1
fi

# Convert to array (portable way without readarray)
RUN_ID_ARRAY=()
while IFS= read -r line; do
    RUN_ID_ARRAY+=("$line")
done <<< "$RUN_IDS"

echo "Found ${#RUN_ID_ARRAY[@]} successful runs:"
for run_id in "${RUN_ID_ARRAY[@]}"; do
    echo "  - Run ID: $run_id"
done
echo

# Download artifacts for each run
for run_id in "${RUN_ID_ARRAY[@]}"; do
    echo "------------------------------------------------------------"
    echo "Processing run ID: $run_id"
    echo "------------------------------------------------------------"

    RUN_DIR="$OUTPUT_DIR/run-$run_id"
    mkdir -p "$RUN_DIR"

    # List all artifacts for this run
    echo "Fetching artifact list..."
    ARTIFACTS=$(gh api "repos/$REPOSITORY/actions/runs/$run_id/artifacts" --jq '.artifacts[] | select(.name | startswith("Test Results (smoke tests)")) | {name: .name, id: .id}')

    if [[ -z "$ARTIFACTS" ]]; then
        echo "Warning: No test result artifacts found for run $run_id"
        continue
    fi

    # Parse artifacts JSON and download each one
    echo "$ARTIFACTS" | jq -c '.' | while read -r artifact; do
        artifact_name=$(echo "$artifact" | jq -r '.name')
        artifact_id=$(echo "$artifact" | jq -r '.id')

        echo "  Downloading: $artifact_name"

        # Determine subdirectory based on artifact name
        if [[ $artifact_name =~ "pytests" ]]; then
            # Extract batch number if present
            if [[ $artifact_name =~ pytests[[:space:]]+([0-9]+) ]]; then
                batch_num="${BASH_REMATCH[1]}"
                artifact_subdir="$RUN_DIR/pytests-$batch_num"
            else
                artifact_subdir="$RUN_DIR/pytests-0"
            fi
        elif [[ $artifact_name =~ "cypress" ]]; then
            # Extract batch number if present
            if [[ $artifact_name =~ cypress[[:space:]]+([0-9]+) ]]; then
                batch_num="${BASH_REMATCH[1]}"
                artifact_subdir="$RUN_DIR/cypress-$batch_num"
            else
                artifact_subdir="$RUN_DIR/cypress-0"
            fi
        else
            artifact_subdir="$RUN_DIR/other"
        fi

        mkdir -p "$artifact_subdir"

        # Download artifact (gh will create a zip file)
        if gh api "repos/$REPOSITORY/actions/artifacts/$artifact_id/zip" > "$artifact_subdir/artifact.zip" 2>/dev/null; then
            # Unzip artifact
            if unzip -q "$artifact_subdir/artifact.zip" -d "$artifact_subdir" 2>/dev/null; then
                rm "$artifact_subdir/artifact.zip"
                echo "    ✓ Downloaded and extracted to: $artifact_subdir"
            else
                echo "    ✗ Failed to extract artifact"
                rm -f "$artifact_subdir/artifact.zip"
            fi
        else
            echo "    ✗ Failed to download artifact"
        fi
    done

    echo "  Completed run $run_id"
    echo
done

echo "============================================================"
echo "Download complete!"
echo "============================================================"
echo "Artifacts saved to: $OUTPUT_DIR"
echo
echo "Directory structure:"
ls -lh "$OUTPUT_DIR" | tail -n +2 | awk '{print "  " $9}'
echo
echo "Next step: Generate test weights with:"
echo "  python .github/scripts/generate_test_weights.py \\"
echo "    --input-dir $OUTPUT_DIR \\"
echo "    --cypress-output ./dev-artifacts/generated-weights/cypress_weights.json \\"
echo "    --pytest-output ./dev-artifacts/generated-weights/pytest_weights.json"
