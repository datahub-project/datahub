#!/usr/bin/env bash
# fetch-rc-notes.sh <RC_TAG> <STABLE_VERSION>
#
# Fetches the RC's release body from GitHub and writes it to the stable
# release's notes file (mirroring the prep notes-dir convention). Prints
# the notes file path on stdout so the caller can pass it to cut-release.sh.
#
# Stable tags the same commit as the RC, so reusing the RC's notes verbatim
# is the default path. Operators can still Read + edit the saved file before
# invoking cut-release.sh if they want to regenerate.
#
# Usage:
#   .agent-skills/oss-release/scripts/fetch-rc-notes.sh v1.5.0.13rc2 v1.5.0.13
#
# Output:
#   stdout : notes file path (.agent-skills/oss-release/notes/release-notes-<stable>.md)
#   stderr : errors
#
# Exit codes:
#   0 — notes saved, non-empty
#   1 — usage error, gh failure, or empty release body

set -uo pipefail

RC_TAG="${1:-}"
STABLE_VERSION="${2:-}"

if [ -z "$RC_TAG" ] || [ -z "$STABLE_VERSION" ]; then
    echo "Usage: $0 <rc-tag> <stable-version>" >&2
    exit 1
fi

NOTES_DIR=".agent-skills/oss-release/notes"
if ! mkdir -p "$NOTES_DIR"; then
    echo "ERROR: could not create $NOTES_DIR" >&2
    exit 1
fi

NOTES_FILE="${NOTES_DIR}/release-notes-${STABLE_VERSION}.md"

if ! gh release view "$RC_TAG" --repo acryldata/datahub --json body --jq '.body' > "$NOTES_FILE" 2>/dev/null; then
    echo "ERROR: could not fetch RC release notes for ${RC_TAG}" >&2
    echo "       Check 'gh release view ${RC_TAG} --repo acryldata/datahub' by hand." >&2
    rm -f "$NOTES_FILE"
    exit 1
fi

if [ ! -s "$NOTES_FILE" ]; then
    echo "ERROR: RC release body for ${RC_TAG} is empty." >&2
    rm -f "$NOTES_FILE"
    exit 1
fi

echo "$NOTES_FILE"
exit 0
