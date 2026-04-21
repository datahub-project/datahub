#!/usr/bin/env bash
# status.sh
#
# Read-only snapshot: find the latest RC tag and run check-ci.sh against
# its SHA. Implementation of the `/oss-release status` subcommand — no tags
# created, nothing published, no confirmation gates.
#
# Consolidated from the multi-line status block at the bottom of
# workflows/finish.md so Claude Code can pre-approve it as a single command.
#
# Usage:
#   .agent-skills/oss-release/scripts/status.sh
#
# Exit codes:
#   0 — ran successfully (check-ci.sh output reflects CI state)
#   1 — no RC tags found, or safe-fetch-tags reported a real failure

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if ! "$SCRIPT_DIR/safe-fetch-tags.sh"; then
    echo "ERROR: safe-fetch-tags reported a real failure." >&2
    exit 1
fi

LATEST_RC=$(git tag -l 'v*rc*' | sort -V | tail -1)
if [ -z "$LATEST_RC" ]; then
    echo "No RC tags found (pattern 'v*rc*')." >&2
    exit 1
fi

RC_SHA=$(git rev-list -n 1 "$LATEST_RC")
echo "Latest RC: ${LATEST_RC}  (${RC_SHA:0:10})"
echo ""

"$SCRIPT_DIR/check-ci.sh" "$RC_SHA"
