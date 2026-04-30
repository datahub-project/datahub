#!/usr/bin/env bash
# prepare-notes.sh <version>
#
# Creates the notes directory, runs the stale-notes detector, and prints the
# notes file path on stdout. The caller captures the printed path and uses it
# for subsequent Write/cut-release.sh calls.
#
# Extracted from prep.md Step 3.5 so Claude Code can classify it as a single
# command instead of a compound bash block (mkdir + var assignment + if-then).
#
# Operates on the caller's cwd — notes/ lives next to the skill, so a worktree
# invocation writes to that worktree's copy of the skill.
#
# Usage:
#   .agent-skills/oss-release/scripts/prepare-notes.sh v1.5.0.14rc1
#
# Output:
#   stdout : notes file path (e.g. .agent-skills/oss-release/notes/release-notes-v1.5.0.14rc1.md)
#   stderr : warnings (stale-notes detection, etc.)
#
# Exit codes:
#   0 — fresh or no existing notes file; caller should proceed to generate
#   2 — stale notes detected; caller must regenerate or explicitly confirm reuse
#   1 — usage error or unexpected failure

set -uo pipefail

VERSION="${1:-}"
if [ -z "$VERSION" ]; then
    echo "Usage: $0 <next-version>  (e.g. v1.5.0.14rc1)" >&2
    exit 1
fi

NOTES_DIR=".agent-skills/oss-release/notes"
if ! mkdir -p "$NOTES_DIR"; then
    echo "ERROR: could not create $NOTES_DIR" >&2
    exit 1
fi

NOTES_FILE="${NOTES_DIR}/release-notes-${VERSION}.md"

# Stale-notes detector — only runs if a notes file already exists.
#
# A release-notes-<version>.md may linger from a prior prep run on the same
# version (e.g. you prep'd, walked away, the branch fast-forwarded, you came
# back). Reusing stale notes silently publishes wrong content — one of the few
# prep failure modes that produces a quiet data-integrity bug rather than a
# loud error.
STALE=false
if [ -f "$NOTES_FILE" ]; then
    STALE_SHA=$(grep -oE '\b[0-9a-f]{10,40}\b' "$NOTES_FILE" | head -1 || true)
    CURRENT_SHA=$(git rev-parse HEAD 2>/dev/null || echo "")
    if [ -n "$STALE_SHA" ] && [ -n "$CURRENT_SHA" ] && [[ "$CURRENT_SHA" != "$STALE_SHA"* ]]; then
        STALE=true
        {
            echo "⚠️  STALE NOTES DETECTED"
            echo "   File:    $NOTES_FILE"
            echo "   Notes reference SHA: $STALE_SHA"
            echo "   Current HEAD:        $CURRENT_SHA"
            echo "   The existing notes were written for a different commit and almost"
            echo "   certainly mis-describe the release. Regenerate, or explicitly"
            echo "   confirm reuse before continuing."
        } >&2
    fi
fi

# stdout gets the path — intentionally the only line on stdout so callers
# can $(...)-capture it cleanly.
echo "$NOTES_FILE"

[ "$STALE" = "true" ] && exit 2
exit 0
