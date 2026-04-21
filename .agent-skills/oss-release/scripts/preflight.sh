#!/usr/bin/env bash
# preflight.sh
#
# Verifies the repo is in a releasable state and emits a one-block summary.
# Extracts the preflight logic from workflows/prep.md Step 0 into a single
# script so the agent only needs to invoke one command instead of running
# a compound bash block (which Claude Code's permission matcher cannot
# classify against prefix patterns — every compound statement prompts).
#
# Does:
#   1. Clean-tree check (tracked files only)
#   2. Fetch origin master + tags (via safe-fetch-tags.sh)
#   3. Verify HEAD == origin/master (release tag must point at a merged commit)
#   4. Detect HEAD already being tagged (degenerate "republish" case)
#   5. Compute latest-stable tag and commit-range count
#   6. Emit preflight summary block + any warnings
#
# Does NOT:
#   - Apply the dry-run vs real-run policy gate (agent's job)
#   - Prompt for confirmation on degenerate cases (agent's job)
#   - Modify any state
#
# Usage:
#   .agent-skills/oss-release/scripts/preflight.sh
#
# Exit codes:
#   0 — clean or clean-with-warnings (agent decides whether to proceed)
#   1 — hard blocker (dirty tree, HEAD != origin/master, fetch failed)

set -uo pipefail

# Keep SCRIPT_DIR so we can invoke peer scripts (safe-fetch-tags.sh). Do NOT
# cd to the script's own repo root — the script may be invoked from a
# different worktree (e.g. /tmp/datahub-master) and must operate on the
# caller's checkout, not the one where the script file happens to live.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Hard blocker: dirty tracked tree.
if ! git diff-index --quiet HEAD --; then
    echo "ERROR: uncommitted changes in tracked files." >&2
    git status --short >&2
    exit 1
fi

# Hard blocker: can't reach origin.
if ! git fetch origin master --quiet; then
    echo "ERROR: fetch origin master failed." >&2
    exit 1
fi

if ! "$SCRIPT_DIR/safe-fetch-tags.sh"; then
    echo "ERROR: safe-fetch-tags reported a real failure." >&2
    exit 1
fi

HEAD_SHA=$(git rev-parse HEAD)
ORIGIN_SHA=$(git rev-parse origin/master)

# Hard blocker: wrong branch or stale checkout.
if [ "$HEAD_SHA" != "$ORIGIN_SHA" ]; then
    echo "ERROR: HEAD does not match origin/master." >&2
    echo "  HEAD         : $HEAD_SHA" >&2
    echo "  origin/master: $ORIGIN_SHA" >&2
    echo "" >&2
    echo "Current branch: $(git rev-parse --abbrev-ref HEAD)" >&2
    echo "Checkout master and run 'git pull origin master' first." >&2
    exit 1
fi

AT_HEAD_TAGS=$(git tag --points-at HEAD 2>/dev/null | grep -E '^v[0-9]' || true)
LATEST_STABLE=$(git tag -l 'v*' | grep -v rc | sort -V | tail -1)
RANGE_COUNT=$(git rev-list --count --no-merges "${LATEST_STABLE}..HEAD" 2>/dev/null || echo "?")

cat <<EOF
=== Preflight summary ===
  HEAD              : $HEAD_SHA
  origin/master     : $ORIGIN_SHA
  latest stable tag : ${LATEST_STABLE} @ $(git rev-parse "$LATEST_STABLE" 2>/dev/null | cut -c1-10)
  commits in range  : ${RANGE_COUNT} non-merge commits since ${LATEST_STABLE}
  tags at HEAD      : ${AT_HEAD_TAGS:-(none)}
EOF

# Soft warnings — agent reads these and applies the dry-run/real-run gate.
if [ -n "$AT_HEAD_TAGS" ]; then
    echo ""
    echo "⚠️  WARNING: HEAD is already tagged as:"
    echo "$AT_HEAD_TAGS" | sed 's/^/      /'
    echo "   A new release here will tag the SAME commit under a different name."
    echo "   Confirm explicitly before continuing."
fi

if [ "$RANGE_COUNT" = "0" ]; then
    echo ""
    echo "⚠️  WARNING: empty range — HEAD is at ${LATEST_STABLE}, nothing new to release."
fi

exit 0
