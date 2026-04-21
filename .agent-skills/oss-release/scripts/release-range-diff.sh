#!/usr/bin/env bash
# release-range-diff.sh
#
# Emits the three-section release-range diff that feeds Step 2's safety
# assessment: commits, file stats, and safety-relevant paths touched since
# the latest stable tag.
#
# Read-only. No args. Operates on the caller's cwd — works correctly from
# any worktree.
#
# Extracted from prep.md Step 2a into a single script so Claude Code's
# permission matcher can classify it as a single command instead of a
# compound bash block.
#
# Usage:
#   .agent-skills/oss-release/scripts/release-range-diff.sh
#
# Exit codes:
#   0 — always (even for empty range; empty range is informational, not an error)

set -uo pipefail

LATEST_STABLE=$(git tag -l 'v*' | grep -v rc | sort -V | tail -1)

if [ -z "$LATEST_STABLE" ]; then
    echo "ERROR: no stable tag found (pattern 'v*' without 'rc')." >&2
    exit 1
fi

RANGE="${LATEST_STABLE}..HEAD"

# Empty-range rendering — collapse three empty sections into one line.
RANGE_COUNT=$(git rev-list --count --no-merges "$RANGE" 2>/dev/null || echo "?")
if [ "$RANGE_COUNT" = "0" ]; then
    echo "=== Release range (${RANGE}) ==="
    echo "  (empty — HEAD is at ${LATEST_STABLE}, nothing to release)"
    echo ""
    echo "Safety assessment: all categories N/A — empty range."
    exit 0
fi

echo "=== Commits in release range (${RANGE}) ==="
git log --oneline --no-merges "$RANGE"
echo ""

echo "=== Files changed (stat) ==="
git diff --stat "$RANGE"
echo ""

echo "=== Safety-relevant paths touched ==="
SAFETY_PATHS=$(
    git diff --name-only "$RANGE" \
        | grep -E '(pyproject\.toml|setup\.py|setup\.cfg|uv\.lock|requirements.*\.txt|metadata-ingestion|datahub-actions|metadata-ingestion-modules)' \
        || true
)
if [ -z "$SAFETY_PATHS" ]; then
    echo "(no safety-relevant paths touched)"
else
    echo "$SAFETY_PATHS"
fi

exit 0
