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
# Defense in depth: before dispatching for real, this script re-runs the
# pypi gate (wait-for-pypi-release.sh) and refuses to dispatch unless it
# exits 0 — i.e. the `pypi-release metadata-ingestion` run for this tag
# succeeded AND its settle window has elapsed. Dispatching inside that
# window is the recurring release race that makes connector jobs fail with
# "No solution found when resolving dependencies". Override with
# --skip-pypi-check when you know better (e.g. an older tag whose
# pypi-release run has aged out of the queryable window).
#
# --dry-run performs no network gating at all: it prints the command it
# would run and exits.
#
# Usage:
#   dispatch-connector-tests.sh <rc-tag> [--dry-run] [--skip-pypi-check]
#
# Environment:
#   CONNECTOR_TESTS_REPO     (default: acryldata/connector-tests)
#   CONNECTOR_TESTS_WORKFLOW (default: nightly_tests.yaml)
#   CONNECTOR_TESTS_REF      (default: main)
#   PYPI_SETTLE_SECONDS      (default: 120) — forwarded to wait-for-pypi-release.sh

set -euo pipefail

CONNECTOR_REPO="${CONNECTOR_TESTS_REPO:-acryldata/connector-tests}"
WORKFLOW_FILE="${CONNECTOR_TESTS_WORKFLOW:-nightly_tests.yaml}"
WORKFLOW_REF="${CONNECTOR_TESTS_REF:-main}"

command -v gh &>/dev/null || { echo "Error: 'gh' required" >&2; exit 1; }
gh auth token &>/dev/null || { echo "Error: gh CLI not authenticated. Run: gh auth login" >&2; exit 1; }

RC_TAG="${1:-}"
DRY_RUN=false
SKIP_PYPI_CHECK=false
shift 1 2>/dev/null || true
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --skip-pypi-check) SKIP_PYPI_CHECK=true ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

if [ -z "$RC_TAG" ]; then
    echo "Usage: $0 <rc-tag> [--dry-run] [--skip-pypi-check]" >&2
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

# --- pypi gate (real runs only) ----------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYPI_GATE="${SCRIPT_DIR}/wait-for-pypi-release.sh"

if [ "$SKIP_PYPI_CHECK" = "true" ]; then
    echo "⚠️  --skip-pypi-check: dispatching without verifying the pypi gate."
    echo "    If the wheel isn't resolvable yet, jobs will fail with"
    echo "    'No solution found when resolving dependencies' (re-run them, don't re-cut)."
    echo ""
elif [ ! -x "$PYPI_GATE" ]; then
    echo "Error: pypi gate not found or not executable: ${PYPI_GATE}" >&2
    echo "  Re-run with --skip-pypi-check to dispatch anyway." >&2
    exit 1
else
    GATE_EC=0
    "$PYPI_GATE" "$RC_TAG" || GATE_EC=$?
    if [ "$GATE_EC" -ne 0 ]; then
        echo ""
        echo "✗ Refusing to dispatch: pypi gate returned ${GATE_EC} (need 0)." >&2
        case "$GATE_EC" in
            2) echo "  No 'pypi-release metadata-ingestion' run for ${RC_TAG} in the queryable" >&2
               echo "  window. Either the release event hasn't fired yet (wait ~30s), or the tag" >&2
               echo "  is old enough that its run has aged out — the legitimate case for the" >&2
               echo "  override below." >&2 ;;
            3) echo "  The pypi-release run is still going. Retry in a minute or two." >&2 ;;
            4) echo "  The pypi-release run did NOT succeed — the wheel is not on pypi." >&2
               echo "  Do not override; investigate the failing run." >&2 ;;
            5) echo "  The wheel was just published; wait out the settle window printed above" >&2
               echo "  (PYPI_SETTLE_SECONDS) and retry." >&2 ;;
        esac
        echo "" >&2
        echo "  Override (rare, human-only): $0 ${RC_TAG} --skip-pypi-check" >&2
        exit "$GATE_EC"
    fi
    echo ""
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
