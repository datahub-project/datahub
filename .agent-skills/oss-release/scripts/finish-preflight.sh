#!/usr/bin/env bash
# finish-preflight.sh
#
# Finish-workflow preflight: freshens local state, discovers the latest RC,
# and reports how far origin/master has advanced past it. Does NOT require
# HEAD == origin/master (stable promotion operates on an existing RC tag,
# not on the current checkout — so HEAD position is irrelevant here).
#
# Consolidates Steps 0+1+2 of workflows/finish.md into a single command so
# Claude Code can classify it against the allowlist without hitting the
# compound-statement rejection.
#
# Usage:
#   .agent-skills/oss-release/scripts/finish-preflight.sh
#
# Exit codes:
#   0 — clean; summary block on stdout (may include a "master ahead" warning)
#   1 — hard blocker: dirty tree, fetch failure, no RC tags found

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Step 0 — freshen state.
if ! git diff-index --quiet HEAD --; then
    echo "ERROR: uncommitted changes in tracked files." >&2
    git status --short >&2
    exit 1
fi

if ! git fetch origin master --quiet; then
    echo "ERROR: fetch origin master failed." >&2
    exit 1
fi

if ! "$SCRIPT_DIR/safe-fetch-tags.sh"; then
    echo "ERROR: safe-fetch-tags reported a real failure." >&2
    exit 1
fi

# Step 1 — find latest RC tag.
LATEST_RC=$(git tag -l 'v*rc*' | sort -V | tail -1)
if [ -z "$LATEST_RC" ]; then
    echo "ERROR: no RC tags found (pattern 'v*rc*'). Cut an RC first via /oss-release rc." >&2
    exit 1
fi

RC_SHA=$(git rev-list -n 1 "$LATEST_RC" 2>/dev/null || echo "")
if [ -z "$RC_SHA" ]; then
    echo "ERROR: could not resolve $LATEST_RC to a SHA." >&2
    exit 1
fi

# Step 2 — ahead-count relative to origin/master.
MASTER_SHA=$(git rev-parse origin/master)
AHEAD=0
if [ "$RC_SHA" != "$MASTER_SHA" ]; then
    AHEAD=$(git rev-list "$RC_SHA..origin/master" --count 2>/dev/null || echo "?")
fi

# Latest stable (filter out RC tags). The stable-notes changelog must be
# regenerated against LAST_STABLE..RC_SHA — individual RC notes only carry
# the narrow RC-to-RC delta produced by the changelog skill, so reusing a
# single RC's body would lose every earlier RC's contribution.
LAST_STABLE=$(git tag -l 'v*' | grep -v rc | sort -V | tail -1)
LAST_STABLE_DISPLAY="${LAST_STABLE:-(none — first release in this line)}"

cat <<EOF
=== Finish preflight ===
  LATEST_RC         : $LATEST_RC
  RC_SHA            : $RC_SHA
  origin/master SHA : $MASTER_SHA
  master ahead by   : $AHEAD commit(s) since $LATEST_RC
  LAST_STABLE       : $LAST_STABLE_DISPLAY
EOF

# Soft warning — agent decides whether master-ahead should block promotion.
if [ "$AHEAD" != "0" ] && [ "$AHEAD" != "?" ]; then
    echo ""
    echo "⚠️  WARNING: origin/master is $AHEAD commit(s) ahead of ${LATEST_RC}."
    echo "   Stable will tag ${RC_SHA:0:10}, not the latest master SHA."
    echo "   To include new commits, cut a new RC first via /oss-release rc."
fi

exit 0
