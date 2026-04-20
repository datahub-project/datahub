#!/usr/bin/env bash
# dispatch-connector-tests.sh
#
# Triggers the Nightly Connector Tests workflow for a given RC tag and
# prints the resulting run URL (parsed from gh's stdout).
#
# Stateless by design — nothing is persisted locally. The run becomes
# queryable via check-connector-tests.sh as soon as its set-version job
# starts (a few seconds after dispatch).
#
# Usage:
#   dispatch-connector-tests.sh <rc-tag> [--dry-run]
#
# Environment:
#   CONNECTOR_TESTS_REPO     (default: acryldata/connector-tests)
#   CONNECTOR_TESTS_WORKFLOW (default: nightly_tests.yaml)
#   CONNECTOR_TESTS_REF      (default: main)

set -euo pipefail

CONNECTOR_REPO="${CONNECTOR_TESTS_REPO:-acryldata/connector-tests}"
WORKFLOW_FILE="${CONNECTOR_TESTS_WORKFLOW:-nightly_tests.yaml}"
WORKFLOW_REF="${CONNECTOR_TESTS_REF:-main}"

command -v gh &>/dev/null || { echo "Error: 'gh' required" >&2; exit 1; }
gh auth token &>/dev/null || { echo "Error: gh CLI not authenticated. Run: gh auth login" >&2; exit 1; }

RC_TAG="${1:-}"
DRY_RUN=false
shift 1 2>/dev/null || true
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

if [ -z "$RC_TAG" ]; then
    echo "Usage: $0 <rc-tag> [--dry-run]" >&2
    exit 1
fi
if ! [[ "$RC_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?(rc[0-9]+)?$ ]]; then
    echo "Error: invalid rc-tag format: $RC_TAG (expected vX.Y.Z[.W][rcN])" >&2
    exit 1
fi

CMD=( gh workflow run "$WORKFLOW_FILE" --repo "$CONNECTOR_REPO" --ref "$WORKFLOW_REF" -f "version=$RC_TAG" )

if [ "$DRY_RUN" = "true" ]; then
    echo "DRY-RUN — would run:"
    echo "  ${CMD[*]}"
    exit 0
fi

echo "=== Dispatching connector tests for ${RC_TAG} ==="
echo "  Repo    : ${CONNECTOR_REPO}"
echo "  Workflow: ${WORKFLOW_FILE} (ref=${WORKFLOW_REF})"
echo ""

CMD_OUTPUT=$("${CMD[@]}" 2>&1)
CMD_EC=$?
echo "$CMD_OUTPUT"
if [ $CMD_EC -ne 0 ]; then
    echo "Error: gh workflow run failed with exit $CMD_EC" >&2
    exit $CMD_EC
fi

# Parse URL from gh stdout (gh >= 2.40 emits it on success).
RUN_URL=$(echo "$CMD_OUTPUT" | grep -oE 'https://github\.com/[^/]+/[^/]+/actions/runs/[0-9]+' | head -1 || true)

echo ""
if [ -n "$RUN_URL" ]; then
    echo "Run: ${RUN_URL}"
else
    echo "Run URL not in gh output (older gh?). Find it with:"
    echo "  gh run list --repo ${CONNECTOR_REPO} --workflow ${WORKFLOW_FILE} --event workflow_dispatch --limit 3"
fi
echo ""
echo "Check status (in a few seconds, once set-version job starts):"
echo "  .agent-skills/oss-release/scripts/check-connector-tests.sh ${RC_TAG}"
