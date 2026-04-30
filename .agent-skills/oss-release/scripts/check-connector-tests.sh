#!/usr/bin/env bash
# check-connector-tests.sh
#
# Determines whether Nightly Connector Tests have passed for a given RC by
# querying GitHub directly. No local state — GitHub's run history is the
# source of truth.
#
# Scans the N most-recent workflow_dispatch runs on
# acryldata/connector-tests/nightly_tests.yaml, fetches each one's
# set-version job log, and finds the MOST RECENT run whose executed
# "Testing version:" echo matches <rc-tag> exactly. The resulting exit
# code reflects that run's status/conclusion.
#
# Usage:
#   check-connector-tests.sh <rc-tag>
#
# Environment:
#   CONNECTOR_TESTS_REPO        (default: acryldata/connector-tests)
#   CONNECTOR_TESTS_WORKFLOW    (default: nightly_tests.yaml)
#   CONNECTOR_TESTS_SCAN_LIMIT  (default: 30)  — number of past dispatch runs to scan
#
# Exit codes:
#   0  a matching run exists and concluded with success
#   2  no matching run in the last $SCAN_LIMIT dispatches
#   3  a matching run exists but is still running
#   4  a matching run exists but did not succeed (failure/cancelled/etc.)
#
# Note on retention: GitHub retains job logs ~90 days. A run older than
# that cannot be verified by this script and will not be matched.

set -euo pipefail

REPO="${CONNECTOR_TESTS_REPO:-acryldata/connector-tests}"
WORKFLOW_FILE="${CONNECTOR_TESTS_WORKFLOW:-nightly_tests.yaml}"
SCAN_LIMIT="${CONNECTOR_TESTS_SCAN_LIMIT:-30}"

for cmd in gh python3; do
    command -v "$cmd" &>/dev/null || { echo "Error: '$cmd' required but not installed." >&2; exit 1; }
done
gh auth token &>/dev/null || { echo "Error: gh CLI is not authenticated. Run: gh auth login" >&2; exit 1; }

RC_TAG="${1:-}"
if [ -z "$RC_TAG" ]; then
    echo "Usage: $0 <rc-tag>" >&2
    exit 1
fi

echo "=== Checking connector tests for ${RC_TAG} ==="
echo "  Source       : ${REPO} / ${WORKFLOW_FILE}"
echo "  Scan depth   : ${SCAN_LIMIT} most-recent workflow_dispatch runs"
echo ""

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FINDER="${SCRIPT_DIR}/_find_connector_run.py"
if [ ! -x "$FINDER" ]; then
    echo "Error: helper script not found or not executable: ${FINDER}" >&2
    exit 1
fi

RUNS_JSON=$(gh run list --repo "$REPO" --workflow "$WORKFLOW_FILE" \
    --event workflow_dispatch --limit "$SCAN_LIMIT" \
    --json databaseId,url,status,conclusion,createdAt 2>/dev/null || echo "[]")

MATCH=$(printf '%s' "$RUNS_JSON" | "$FINDER" "$RC_TAG" "$REPO")

if [ -z "$MATCH" ]; then
    echo ""
    echo "✗ No run in the last ${SCAN_LIMIT} workflow_dispatch runs matches 'Testing version: ${RC_TAG}'"
    echo ""
    echo "  To dispatch one now:"
    echo "    .agent-skills/oss-release/scripts/dispatch-connector-tests.sh ${RC_TAG}"
    echo ""
    echo "  Or increase scan depth (older runs):"
    echo "    CONNECTOR_TESTS_SCAN_LIMIT=60 $0 ${RC_TAG}"
    exit 2
fi

RUN_ID=$(echo "$MATCH" | python3 -c "import json,sys;print(json.load(sys.stdin)['run_id'])")
RUN_URL=$(echo "$MATCH" | python3 -c "import json,sys;print(json.load(sys.stdin)['url'])")
STATUS=$(echo "$MATCH" | python3 -c "import json,sys;print(json.load(sys.stdin)['status'])")
CONCLUSION=$(echo "$MATCH" | python3 -c "import json,sys;print(json.load(sys.stdin).get('conclusion') or '')")
CREATED_AT=$(echo "$MATCH" | python3 -c "import json,sys;print(json.load(sys.stdin)['created_at'])")
SCAN_POS=$(echo "$MATCH" | python3 -c "import json,sys;print(json.load(sys.stdin)['scan_position'])")

echo ""
echo "✓ Match: ${RUN_URL}"
echo "    run_id        : ${RUN_ID}"
echo "    created_at    : ${CREATED_AT}"
echo "    status        : ${STATUS}"
echo "    conclusion    : ${CONCLUSION:-(pending)}"
echo "    position      : ${SCAN_POS}/${SCAN_LIMIT} (scan order — newest first)"
echo ""

case "$STATUS" in
    completed)
        if [ "$CONCLUSION" = "success" ]; then
            echo "✓ Connector tests passed for ${RC_TAG}"
            exit 0
        else
            echo "✗ Connector tests concluded with '${CONCLUSION}' — not 'success'"
            echo "  Inspect: ${RUN_URL}"
            exit 4
        fi
        ;;
    in_progress|queued|waiting|pending|requested)
        echo "… Connector tests still running (${STATUS})"
        echo "  Watch: ${RUN_URL}"
        exit 3
        ;;
    *)
        echo "? Unknown status '${STATUS}' — treating as not-yet-passing"
        exit 4
        ;;
esac
