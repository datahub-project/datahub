#!/bin/bash
#
# Download test artifacts from recent successful CI runs.
#
# This script uses the GitHub CLI (gh) to fetch workflow runs and download
# test result artifacts, organizing them by run ID for later processing.

set -euxo pipefail

# Default values
OUTPUT_DIR="./dev-artifacts/test-results"
RUN_COUNT=3
WORKFLOW_NAME="docker-unified.yml"
REPOSITORY=""
# Artifact name prefix to download. Default matches smoke-test result artifacts; override for
# other workflows (e.g. "build-and-test" for Gradle JUnit from build-and-test.yml).
ARTIFACT_PREFIX="Test Results (smoke tests)"
ALLOW_FAILED=false
NO_FAIL_ON_EMPTY=false
# When fewer than RUN_COUNT qualifying runs are found in the recent page, widen
# the lookback window to at least this many days so a quiet stretch doesn't
# starve the weight harvest.
MIN_LOOKBACK_DAYS=3

# Parse arguments
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Download test artifacts from recent CI runs.

OPTIONS:
    --output-dir DIR      Output directory for artifacts (default: ./dev-artifacts/test-results)
    --run-count N         Number of recent runs to download (default: 3)
    --workflow NAME       Workflow file name (default: docker-unified.yml)
    --repository REPO     Repository in format owner/repo (default: auto-detect from git)
    --artifact-prefix STR Artifact name prefix to download (default: "Test Results (smoke tests)")
    --allow-failed        Also harvest completed-but-failed runs (default: off, success-only).
                          Only success and failure conclusions are selected; cancelled and
                          in-progress runs are always skipped.
    --no-fail-on-empty    Exit 0 and print a "NO_DATA=true" sentinel when no qualifying runs
                          or no artifacts are found (default: off, exits 1 on no runs).
    -h, --help            Show this help message

REQUIREMENTS:
    - GitHub CLI (gh) must be installed and authenticated
    - Must be run from within a git repository (unless --repository is specified)

EXAMPLE:
    # Smoke test weights (default prefix)
    $0 --output-dir ./dev-artifacts/test-results --run-count 3

    # Ingestion integration test weights (harvest per-batch junit artifacts)
    $0 --output-dir ./test-artifacts --run-count 3 \
        --workflow metadata-ingestion.yml --artifact-prefix "metadata-ingestion-test-results" \
        --allow-failed --no-fail-on-empty
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
        --artifact-prefix)
            ARTIFACT_PREFIX="$2"
            shift 2
            ;;
        --repository)
            REPOSITORY="$2"
            shift 2
            ;;
        --allow-failed)
            ALLOW_FAILED=true
            shift
            ;;
        --no-fail-on-empty)
            NO_FAIL_ON_EMPTY=true
            shift
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
echo "Artifact prefix: $ARTIFACT_PREFIX"
echo "Output directory: $OUTPUT_DIR"
if [[ "$ALLOW_FAILED" == "true" ]]; then
    echo "Mode: success + failure runs (--allow-failed)"
else
    echo "Mode: success-only runs"
fi
if [[ "$NO_FAIL_ON_EMPTY" == "true" ]]; then
    echo "Empty handling: soft-skip with NO_DATA sentinel (--no-fail-on-empty)"
else
    echo "Empty handling: fail on no runs"
fi
echo "============================================================"
echo

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Build the run-selection jq filter. With --allow-failed we also harvest
# completed-but-failed runs (e.g. a single flaky batch failing the whole run);
# cancelled and in-progress runs are never selected.
if [[ "$ALLOW_FAILED" == "true" ]]; then
    RUN_FILTER='select((.conclusion=="success" or .conclusion=="failure") and .head_branch=="master")'
else
    RUN_FILTER='select(.conclusion=="success" and .head_branch=="master")'
fi

# Fetch recent successful workflow runs from master branch.
#
# We want RUN_COUNT qualifying runs, but also look back at least
# MIN_LOOKBACK_DAYS so a quiet few days don't under-harvest. Strategy: grab the
# most recent page of runs first (cheap, ~30 items, already newest-first). If
# it already yields RUN_COUNT qualifying runs, use them; otherwise widen to the
# last MIN_LOOKBACK_DAYS (paginated) and merge with what we already have.
#
# Limiting is done inside jq (not via `head`) so `gh api` is never killed by
# SIGPIPE under `set -o pipefail`. Phase 2 is best-effort: transient GitHub API
# failures (5xx/timeout) are retried, and if phase 2 still fails we fall back to
# the phase 1 runs instead of aborting.
echo "Fetching recent workflow runs from master branch..."

# gh api with retry on transient failures (5xx, timeouts). Returns non-zero
# only if every attempt fails.
gh_api_retry() {
    local max_attempts=3 attempt=1 delay=5 rc=0
    while [ "$attempt" -le "$max_attempts" ]; do
        if gh api "$@"; then
            return 0
        fi
        rc=$?
        if [ "$attempt" -lt "$max_attempts" ]; then
            echo "gh api call failed (exit $rc); retrying in ${delay}s..." >&2
            sleep "$delay"
            delay=$((delay * 2))
        fi
        attempt=$((attempt + 1))
    done
    return "$rc"
}

# Phase 1: most recent page (cheap, ~30 items, already newest-first).
PHASE1_FILE=$(mktemp)
if ! gh_api_retry "repos/$REPOSITORY/actions/workflows/$WORKFLOW_NAME/runs" > "$PHASE1_FILE"; then
    rm -f "$PHASE1_FILE"
    echo "Error: Failed to fetch workflow runs from GitHub API" >&2
    exit 1
fi
RUNS_JSON=$(jq -c --argjson n "$RUN_COUNT" \
    '[.workflow_runs[] | '"$RUN_FILTER"' | {id: .id, created: .created_at}][0:$n]' \
    "$PHASE1_FILE")
rm -f "$PHASE1_FILE"

QUALIFYING_COUNT=$(printf '%s' "$RUNS_JSON" | jq 'length')

if [ "$QUALIFYING_COUNT" -lt "$RUN_COUNT" ]; then
    # date fallback: GNU (`-d`) on the runner, BSD (`-v`) for local macOS dev.
    SINCE_DATE=$(date -u -d "$MIN_LOOKBACK_DAYS days ago" +%Y-%m-%d 2>/dev/null \
        || date -u -v-${MIN_LOOKBACK_DAYS}d +%Y-%m-%d)
    echo "Only $QUALIFYING_COUNT qualifying run(s) in the recent page; widening to runs since $SINCE_DATE..."
    PHASE2_FILE=$(mktemp)
    # `gh api --paginate` streams one JSON object per page; `jq -s` slurps them
    # into a single array so we can sort and slice across the whole window.
    # The `created>=` filter goes in the URL query string (not `-f`): gh's field
    # flag URL-encodes `>` and GitHub 404s on the encoded form.
    if gh_api_retry --paginate "repos/$REPOSITORY/actions/workflows/$WORKFLOW_NAME/runs?created=>=$SINCE_DATE" > "$PHASE2_FILE"; then
        PHASE2=$(jq -s --argjson n "$RUN_COUNT" \
            '[.[] | .workflow_runs[]? | '"$RUN_FILTER"' | {id: .id, created: .created_at}]
             | sort_by(.created) | reverse | .[0:$n]' \
            "$PHASE2_FILE")
        # Merge phase 1 + phase 2, dedup by id, keep newest RUN_COUNT. Phase 1
        # runs are preserved even if phase 2 found nothing (e.g. the only
        # qualifying runs are older than MIN_LOOKBACK_DAYS).
        RUNS_JSON=$(printf '%s\n%s\n' "$RUNS_JSON" "$PHASE2" \
            | jq -sr --argjson n "$RUN_COUNT" \
                'add | unique_by(.id) | sort_by(.created) | reverse | .[0:$n]')
    else
        echo "Warning: phase 2 widening fetch failed; falling back to phase 1 ($QUALIFYING_COUNT qualifying run(s))." >&2
    fi
    rm -f "$PHASE2_FILE"
fi

RUN_IDS=$(printf '%s' "$RUNS_JSON" | jq -r '.[].id')

if [[ -z "$RUN_IDS" ]]; then
    if [[ "$NO_FAIL_ON_EMPTY" == "true" ]]; then
        echo "NO_DATA=true"
        exit 0
    fi
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

# Track total artifacts successfully extracted across all runs. The download
# loop below runs in a subshell (piped input), so a counter file is used to
# propagate the count back to this shell for the empty-data check.
DOWNLOAD_COUNT_FILE=$(mktemp)
echo 0 > "$DOWNLOAD_COUNT_FILE"

# Download artifacts for each run
for run_id in "${RUN_ID_ARRAY[@]}"; do
    echo "------------------------------------------------------------"
    echo "Processing run ID: $run_id"
    echo "------------------------------------------------------------"

    RUN_DIR="$OUTPUT_DIR/run-$run_id"
    mkdir -p "$RUN_DIR"

    # List all artifacts for this run
    echo "Fetching artifact list..."
    # Pass the prefix via jq --arg (not shell interpolation) so a quote/backslash in it can't
    # break or inject into the jq program. Pipe to standalone jq since gh's --jq takes no --arg.
    ARTIFACTS=$(gh api --paginate "repos/$REPOSITORY/actions/runs/$run_id/artifacts" \
        | jq -c --arg prefix "$ARTIFACT_PREFIX" '.artifacts[] | select(.name | startswith($prefix)) | {name: .name, id: .id}')

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
                echo $(( $(cat "$DOWNLOAD_COUNT_FILE") + 1 )) > "$DOWNLOAD_COUNT_FILE"
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

TOTAL_DOWNLOADS=$(cat "$DOWNLOAD_COUNT_FILE")
rm -f "$DOWNLOAD_COUNT_FILE"

# Soft-skip when no artifacts were harvested across all runs.
if [[ "$TOTAL_DOWNLOADS" -eq 0 ]]; then
    if [[ "$NO_FAIL_ON_EMPTY" == "true" ]]; then
        echo "NO_DATA=true"
        exit 0
    fi
    echo "Error: No test result artifacts were downloaded from any run"
    exit 1
fi

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
