#!/usr/bin/env bash
# safe-fetch-tags.sh
#
# Fetches tags from origin, distinguishing benign "would clobber" failures on
# legacy tags (predating the current 4-segment numbering scheme) from real
# errors that need attention.
#
# Why this exists:
#   The acryldata fork carries ~60 local-only tags from very old release lines
#   (v0.x, v1.1.x, v1.3.x, v1.4.x) that diverged from upstream long ago. These
#   tags are never inputs to current release calculations, but git's default
#   `--tags` fetch refuses to overwrite them and emits warnings every time.
#   Constant noise trains agents to ignore warnings — including ones that
#   actually matter. This wrapper fixes that.
#
# Behavior:
#   - If fetch succeeds clean → silent success.
#   - If only failures are "would clobber" on tags that DON'T match the current
#     release line pattern (4-segment vX.Y.Z.W[rcN]) → quiet success with a
#     one-line note about how many legacy tags were skipped.
#   - If failures include clobbers on current-line tags, OR any other failure
#     mode → loud warning with the full stderr.
#
# Usage:
#   .agent-skills/oss-release/scripts/safe-fetch-tags.sh
#
# Exit code:
#   0 on success (silent or with note)
#   1 on real failure (always loud)

set -euo pipefail

CURRENT_LINE_PATTERN='^v[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(rc[0-9]+)?$'

TMP_ERR=$(mktemp)
trap 'rm -f "$TMP_ERR"' EXIT

if git fetch origin --tags 2>"$TMP_ERR"; then
    # Some clobber attempts produce non-zero rc; some don't, depending on git
    # version. Check stderr for the rejection lines either way.
    if ! grep -q 'rejected' "$TMP_ERR"; then
        # Clean success.
        exit 0
    fi
fi

# Fetch reported issues. Classify them.
REJECTED=$(grep -E '\[rejected\]' "$TMP_ERR" | awk '{print $2}' || true)
NON_CLOBBER=$(grep -v -E '(\[rejected\].*would clobber|^From |^$)' "$TMP_ERR" | grep -v '^[[:space:]]*+' || true)

if [ -n "$NON_CLOBBER" ]; then
    echo "Warning: git fetch origin --tags reported errors beyond legacy tag clobbers:" >&2
    cat "$TMP_ERR" >&2
    exit 1
fi

# All failures are "would clobber". Classify by whether they hit the current line.
IN_LINE=$(echo "$REJECTED" | grep -E "$CURRENT_LINE_PATTERN" || true)
LEGACY_COUNT=$(echo "$REJECTED" | grep -v -E "$CURRENT_LINE_PATTERN" | grep -c . || true)

if [ -n "$IN_LINE" ]; then
    echo "Warning: git fetch refused to overwrite tags in the CURRENT release line:" >&2
    echo "$IN_LINE" | sed 's/^/  /' >&2
    echo "  These divergences could affect version detection. Investigate before continuing." >&2
    exit 1
fi

# Only legacy clobbers — quiet success.
if [ "$LEGACY_COUNT" -gt 0 ]; then
    echo "Refreshed origin tags (skipped ${LEGACY_COUNT} divergent legacy tags from pre-4-segment release lines)." >&2
fi
exit 0
