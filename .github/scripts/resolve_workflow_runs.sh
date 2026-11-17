#!/bin/bash
#
# Resolve the newest qualifying GitHub Actions workflow runs.
#
# Filtered workflow-run queries (branch=, status=, event=) time out past a few
# thousand matches and can return a created-desc page whose newest run is months
# old. Always bound the search with created>=. Start at --min-lookback-days and
# widen to --max-lookback-days when that window has fewer than --run-count
# qualifying runs.
# https://github.blog/changelog/2026-09-25-changes-to-query-results-in-the-github-actions-api-and-ui/
#
# Prints a JSON array to stdout:
#   [{"id":..., "head_sha":..., "created_at":..., "html_url":...}, ...]
# Progress and errors go to stderr.

set -euo pipefail

WORKFLOW_NAME=""
REPOSITORY=""
BRANCH="master"
EVENT=""
RUN_COUNT=1
ALLOW_FAILED=false
NO_FAIL_ON_EMPTY=false
MIN_LOOKBACK_DAYS=3
MAX_LOOKBACK_DAYS=7

usage() {
    local exit_code="${1:-1}"
    cat <<EOF
Usage: $0 --workflow NAME --repository OWNER/REPO [OPTIONS]

Print the newest qualifying workflow runs as a JSON array.

OPTIONS:
    --workflow NAME            Workflow file name (required)
    --repository OWNER/REPO    Repository (required)
    --branch BRANCH            Head branch (default: master)
    --event EVENT              Workflow event, e.g. push (default: any event)
    --run-count N              How many newest runs to return (default: 1)
    --allow-failed             Include completed runs whose conclusion is failure.
                               Cancelled and in-progress runs are never selected.
    --no-fail-on-empty         Print [] and exit 0 when no run qualifies
                               (default: exit 1)
    --min-lookback-days N      Initial created>= window (default: 3)
    --max-lookback-days N      Wider window used when the initial window is short
                               (default: 7)
    -h, --help                 Show this help message
EOF
    exit "$exit_code"
}

# Value-taking flags must not read $2 when it is absent: set -u would abort
# with "unbound variable" instead of this script's validation error.
require_value() {
    local flag="$1"
    if [[ $# -lt 2 ]]; then
        echo "Error: ${flag} requires a value" >&2
        usage 1
    fi
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --workflow)
            require_value "$@"
            WORKFLOW_NAME="$2"
            shift 2
            ;;
        --repository)
            require_value "$@"
            REPOSITORY="$2"
            shift 2
            ;;
        --branch)
            require_value "$@"
            BRANCH="$2"
            shift 2
            ;;
        --event)
            require_value "$@"
            EVENT="$2"
            shift 2
            ;;
        --run-count)
            require_value "$@"
            RUN_COUNT="$2"
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
        --min-lookback-days)
            require_value "$@"
            MIN_LOOKBACK_DAYS="$2"
            shift 2
            ;;
        --max-lookback-days)
            require_value "$@"
            MAX_LOOKBACK_DAYS="$2"
            shift 2
            ;;
        -h|--help)
            usage 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage 1
            ;;
    esac
done

if [[ -z "$WORKFLOW_NAME" || -z "$REPOSITORY" ]]; then
    echo "Error: --workflow and --repository are required" >&2
    usage 1
fi

if ! [[ "$RUN_COUNT" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --run-count must be a positive integer, got '${RUN_COUNT}'" >&2
    exit 1
fi

if ! [[ "$MIN_LOOKBACK_DAYS" =~ ^[1-9][0-9]*$ && "$MAX_LOOKBACK_DAYS" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: lookback days must be positive integers" >&2
    exit 1
fi

if ! command -v gh > /dev/null; then
    echo "Error: GitHub CLI (gh) is not installed." >&2
    echo "Install from: https://cli.github.com/" >&2
    exit 1
fi

if ! command -v jq > /dev/null; then
    echo "Error: jq is not installed." >&2
    exit 1
fi

if ! gh auth status > /dev/null 2>&1; then
    echo "Error: GitHub CLI is not authenticated." >&2
    echo "Run: gh auth login" >&2
    exit 1
fi

# gh api with retry on transient failures (5xx, timeouts). Returns non-zero
# only if every attempt fails.
gh_api_retry() {
    local max_attempts=3 attempt=1 delay=5 rc=0
    while [ "$attempt" -le "$max_attempts" ]; do
        # $? after a failed `if` is the status of the `if`, which is 0.
        # Capture the command status in the else branch.
        if gh api "$@"; then
            return 0
        else
            rc=$?
        fi
        if [ "$attempt" -lt "$max_attempts" ]; then
            echo "gh api call failed (exit $rc); retrying in ${delay}s..." >&2
            sleep "$delay"
            delay=$((delay * 2))
        fi
        attempt=$((attempt + 1))
    done
    return "$rc"
}

# GNU date (`-d`) on the runner, BSD date (`-v`) for local macOS dev.
utc_days_ago() {
    date -u -d "$1 days ago" +%Y-%m-%d 2>/dev/null \
        || date -u -v-"$1"d +%Y-%m-%d
}

# Print a JSON array of the newest RUN_COUNT qualifying runs created on or after
# SINCE_DATE. `gh api --paginate` streams one JSON object per page; `jq -s`
# slurps them. The created>= filter stays in the URL (not `-f`): gh's field flag
# URL-encodes `>` and GitHub 404s on the encoded form.
fetch_qualifying_runs_since() {
    local since_date="$1"
    local page_file query allow_failed_json
    page_file=$(mktemp)
    query="repos/${REPOSITORY}/actions/workflows/${WORKFLOW_NAME}/runs?branch=${BRANCH}&per_page=100&created=>=${since_date}"
    if [[ -n "$EVENT" ]]; then
        query="${query}&event=${EVENT}"
    fi
    if [[ "$ALLOW_FAILED" == "true" ]]; then
        allow_failed_json=true
    else
        allow_failed_json=false
    fi
    if ! gh_api_retry --paginate "$query" > "$page_file"; then
        rm -f "$page_file"
        return 1
    fi
    if ! jq -sec \
        --argjson n "$RUN_COUNT" \
        --arg branch "$BRANCH" \
        --arg event "$EVENT" \
        --argjson allow_failed "$allow_failed_json" \
        '[.[] | .workflow_runs[]?
          | select(
              (if $allow_failed then
                 (.conclusion == "success" or .conclusion == "failure")
               else
                 .conclusion == "success"
               end)
              and .head_branch == $branch
              and ($event == "" or .event == $event)
            )
          | {id: .id, head_sha: .head_sha, created_at: .created_at, html_url: .html_url}
         ]
         | sort_by(.created_at) | reverse | .[0:$n]' \
        "$page_file"; then
        rm -f "$page_file"
        return 1
    fi
    rm -f "$page_file"
}

SINCE_DATE=$(utc_days_ago "$MIN_LOOKBACK_DAYS")
echo "Selecting up to ${RUN_COUNT} ${WORKFLOW_NAME} run(s) on ${BRANCH} since ${SINCE_DATE}..." >&2
if ! RUNS_JSON=$(fetch_qualifying_runs_since "$SINCE_DATE"); then
    echo "Error: Failed to fetch workflow runs from GitHub API" >&2
    exit 1
fi
QUALIFYING_COUNT=$(printf '%s' "$RUNS_JSON" | jq 'length')

if [ "$QUALIFYING_COUNT" -lt "$RUN_COUNT" ] && [ "$MAX_LOOKBACK_DAYS" -gt "$MIN_LOOKBACK_DAYS" ]; then
    WIDER_DATE=$(utc_days_ago "$MAX_LOOKBACK_DAYS")
    echo "Only ${QUALIFYING_COUNT} qualifying run(s) since ${SINCE_DATE}; widening to ${WIDER_DATE}..." >&2
    if WIDER_JSON=$(fetch_qualifying_runs_since "$WIDER_DATE"); then
        RUNS_JSON="$WIDER_JSON"
        SINCE_DATE="$WIDER_DATE"
        QUALIFYING_COUNT=$(printf '%s' "$RUNS_JSON" | jq 'length')
    else
        echo "Warning: lookback to ${WIDER_DATE} failed; keeping runs since ${SINCE_DATE}." >&2
    fi
fi

if [ "$QUALIFYING_COUNT" -eq 0 ]; then
    if [[ "$NO_FAIL_ON_EMPTY" == "true" ]]; then
        printf '[]\n'
        exit 0
    fi
    echo "Error: No qualifying ${WORKFLOW_NAME} runs on ${BRANCH} since ${SINCE_DATE}" >&2
    exit 1
fi

printf '%s\n' "$RUNS_JSON"
