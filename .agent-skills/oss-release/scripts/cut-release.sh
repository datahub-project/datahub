#!/usr/bin/env bash
# cut-release.sh
#
# Creates an official (non-prerelease) GitHub release tag.
#
# Usage:
#   .agent-skills/oss-release/scripts/cut-release.sh <version> <sha> <notes-file>
#
#   version    : e.g. "1.5.0.7"  (without the leading "v")
#   sha        : full or short commit SHA to tag
#   notes-file : path to a markdown file containing release notes
#
# Prerequisites:
#   - gh CLI authenticated
#   - git with push access to origin

set -euo pipefail

# --- Prerequisites check ---
for cmd in git gh; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: '$cmd' is required but not installed." >&2
        [ "$cmd" = "gh" ] && echo "  Install: https://cli.github.com" >&2
        exit 1
    fi
done
if ! gh auth status &>/dev/null; then
    echo "Error: gh CLI is not authenticated. Run: gh auth login" >&2
    exit 1
fi

VERSION="${1:-}"
SHA="${2:-}"
NOTES_FILE="${3:-}"

if [ -z "$VERSION" ] || [ -z "$SHA" ] || [ -z "$NOTES_FILE" ]; then
    echo "Usage: $0 <version> <sha> <notes-file>"
    echo "  version    : e.g. 1.5.0.7 (no leading 'v')"
    echo "  sha        : commit to tag"
    echo "  notes-file : markdown file with release notes body"
    exit 1
fi

TAG="v${VERSION}"

if [ ! -f "$NOTES_FILE" ]; then
    echo "Error: notes file not found: ${NOTES_FILE}"
    exit 1
fi

echo "=== Cutting official release: ${TAG} ==="
echo "  Commit : ${SHA}"
echo "  Notes  : ${NOTES_FILE}"
echo ""

# Create and push tag
echo "[1/3] Creating tag ${TAG} at ${SHA} ..."
git tag "$TAG" "$SHA"
git push origin "$TAG"
echo "      Tag pushed."
echo ""

# Create GitHub release
echo "[2/3] Creating GitHub release ..."
RELEASE_URL=$(gh release create "$TAG" \
    --title "$TAG" \
    --notes-file "$NOTES_FILE")
echo "      Release URL: ${RELEASE_URL}"
echo ""

echo "[3/3] Done."
echo ""
echo "Release: ${RELEASE_URL}"
