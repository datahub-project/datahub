#!/usr/bin/env bash
# cut-release.sh
#
# Creates a GitHub release: annotated git tag + push + gh release create.
#
# Usage:
#   cut-release.sh <version> <sha> <notes-file> [--prerelease] [--dry-run]
#
#   version      : e.g. "1.5.0.8rc1"  (without leading "v")
#   sha          : full or short commit SHA to tag
#   notes-file   : path to markdown file containing the release body
#   --prerelease : mark as GitHub pre-release (use for RC versions)
#   --dry-run    : print every git/gh command that would run, but execute nothing
#
# Environment variables (optional overrides):
#   GITHUB_REPO_OWNER   release repo owner  (default: acryldata)
#   GITHUB_REPO_NAME    release repo name   (default: datahub)
#
# Prerequisites:
#   - gh CLI authenticated
#   - git with push access to origin

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "$REPO_ROOT"

for cmd in git gh; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: '$cmd' is required but not installed." >&2
        [ "$cmd" = "gh" ] && echo "  Install: https://cli.github.com" >&2
        exit 1
    fi
done
if ! gh auth token &>/dev/null; then
    echo "Error: gh CLI is not authenticated. Run: gh auth login" >&2
    exit 1
fi

REPO_OWNER="${GITHUB_REPO_OWNER:-acryldata}"
REPO_NAME="${GITHUB_REPO_NAME:-datahub}"

# Parse args (positional 1-3 + optional flags in any order after)
VERSION="${1:-}"
SHA="${2:-}"
NOTES_FILE="${3:-}"
PRERELEASE=false
DRY_RUN=false
shift 3 2>/dev/null || true
for arg in "$@"; do
    case "$arg" in
        --prerelease) PRERELEASE=true ;;
        --dry-run)    DRY_RUN=true ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

if [ -z "$VERSION" ] || [ -z "$SHA" ] || [ -z "$NOTES_FILE" ]; then
    echo "Usage: $0 <version> <sha> <notes-file> [--prerelease] [--dry-run]"
    echo "  version    : e.g. 1.5.0.8rc1 (no leading 'v')"
    echo "  sha        : commit to tag"
    echo "  notes-file : markdown file with release notes body"
    exit 1
fi

# Validate version format to prevent malformed tags and injection
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?(rc[0-9]+)?$ ]]; then
    echo "Error: invalid version format: $VERSION" >&2
    echo "Expected: X.Y.Z.W[rcN] or X.Y.Z[rcN]  (no leading 'v')" >&2
    exit 1
fi

TAG="v${VERSION}"

if [ ! -f "$NOTES_FILE" ]; then
    echo "Error: notes file not found: ${NOTES_FILE}" >&2
    exit 1
fi

if [ "$DRY_RUN" = "true" ]; then
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                    DRY-RUN MODE                            ║"
    echo "║  No tags will be created, no releases will be published.   ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
fi

echo "=== Cutting release: ${TAG} ==="
echo "  Repo       : ${REPO_OWNER}/${REPO_NAME}"
echo "  Commit     : ${SHA}"
echo "  Notes      : ${NOTES_FILE}"
echo "  Pre-release: ${PRERELEASE}"
echo ""

# Abort if the tag already exists (locally or on origin) — avoids confusing re-runs
if git rev-parse "$TAG" &>/dev/null; then
    echo "Error: tag ${TAG} already exists locally. Delete with: git tag -d ${TAG}" >&2
    exit 1
fi
if git ls-remote --tags origin "refs/tags/${TAG}" | grep -q "$TAG"; then
    echo "Error: tag ${TAG} already exists on origin. Delete with: git push origin --delete ${TAG}" >&2
    exit 1
fi

# Build the gh release command once so it can be printed or executed
RELEASE_ARGS=(
    "$TAG"
    --repo "${REPO_OWNER}/${REPO_NAME}"
    --title "$TAG"
    --notes-file "$NOTES_FILE"
)
[ "$PRERELEASE" = "true" ] && RELEASE_ARGS+=(--prerelease)

if [ "$DRY_RUN" = "true" ]; then
    echo "[1/3] WOULD run:"
    echo "      git tag -a \"${TAG}\" \"${SHA}\" -m \"Release ${TAG}\""
    echo "      git push origin \"${TAG}\""
    echo ""
    echo "[2/3] WOULD run:"
    echo "      gh release create ${RELEASE_ARGS[*]}"
    echo ""

    # Show what's actually in the release so reviewers catch empty-delta mistakes.
    # Pick the most recent tag reachable from SHA but NOT pointing at SHA itself.
    # Filter to the 4-segment pattern this release line uses (X.Y.Z.W) so
    # ancient 3-segment tags like v1.5.0rc1 don't shadow the right prior tag.
    #
    # Prefer the most recent STABLE tag (no rcN suffix). Stable tags are the
    # canonical "release boundary" — using them in the report keeps the prior-tag
    # label consistent with the preflight summary in SKILL.md (which also uses the
    # latest stable). Fall back to RC only if no stable is reachable, which only
    # happens during the first release of a brand-new line.
    AT_SHA_TAGS_FILE=$(mktemp)
    git tag --points-at "$SHA" 2>/dev/null > "$AT_SHA_TAGS_FILE" || true
    PRIOR_TAG=""
    PRIOR_TAG_FALLBACK=""
    while IFS= read -r tag; do
        [ -z "$tag" ] && continue
        # 4-segment pattern; capture stable separately from RC.
        if [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            if ! grep -Fxq "$tag" "$AT_SHA_TAGS_FILE"; then
                PRIOR_TAG="$tag"
                break
            fi
        elif [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+rc[0-9]+$ ]]; then
            if [ -z "$PRIOR_TAG_FALLBACK" ] && ! grep -Fxq "$tag" "$AT_SHA_TAGS_FILE"; then
                PRIOR_TAG_FALLBACK="$tag"
            fi
        fi
    done < <(git tag --merged "$SHA" --sort=-v:refname 2>/dev/null)
    rm -f "$AT_SHA_TAGS_FILE"
    [ -z "$PRIOR_TAG" ] && PRIOR_TAG="$PRIOR_TAG_FALLBACK"

    # If any release tag is already AT this SHA, the commit range we're about to
    # show is the range that went into THAT tag, NOT new content for the tag
    # we're cutting. Flag this loudly above the range so operators don't mistake
    # already-shipped commits for fresh content.
    AT_SHA_RELEASE_TAGS=$(git tag --points-at "$SHA" 2>/dev/null | grep -E '^v[0-9]' || true)
    if [ -n "$AT_SHA_RELEASE_TAGS" ]; then
        echo "⚠️  WARNING: ${SHA:0:10} is already tagged as:"
        echo "$AT_SHA_RELEASE_TAGS" | sed 's/^/      /'
        echo "   Cutting ${TAG} here would tag the same commit under another name."
        echo "   The commit range below is the range that went into the tag(s) above —"
        echo "   NOT new content being released. Abort unless this is an intentional republish."
        echo ""
    fi

    if [ -n "$PRIOR_TAG" ]; then
        RANGE_COUNT=$(git rev-list --count --no-merges "${PRIOR_TAG}..${SHA}" 2>/dev/null || echo "?")
        echo "--- Commit range: ${PRIOR_TAG}..${SHA:0:10} (${RANGE_COUNT} non-merge commits) ---"
        if [ "$RANGE_COUNT" = "0" ]; then
            echo "⚠️  WARNING: zero non-merge commits since ${PRIOR_TAG}."
            echo "   This release would tag the same commit ${PRIOR_TAG} points at."
            echo "   Intended? If not, abort and wait for new commits on master."
        else
            git log "${PRIOR_TAG}..${SHA}" --oneline --no-merges | head -30
            [ "$RANGE_COUNT" -gt 30 ] && echo "  ... (+$((RANGE_COUNT - 30)) more)"
        fi
        echo "--- end commit range ---"
        echo ""
    fi

    echo "--- Notes body preview ---"
    cat "$NOTES_FILE"
    echo "--- end notes preview ---"
    echo ""
    echo "[3/3] Dry-run complete. Nothing was changed."
    exit 0
fi

# If we fail after pushing the tag but before the release is created,
# roll back the pushed tag so the user can safely re-run.
TAG_PUSHED=false
cleanup_on_failure() {
    local ec=$?
    if [ "$ec" -ne 0 ] && [ "$TAG_PUSHED" = "true" ]; then
        echo "" >&2
        echo "Release creation failed. Rolling back pushed tag ${TAG} ..." >&2
        git push origin --delete "$TAG" 2>/dev/null || echo "  (could not delete remote tag — delete manually)" >&2
        git tag -d "$TAG" 2>/dev/null || true
    fi
    return "$ec"
}
trap cleanup_on_failure EXIT

# Annotated tag so the tag carries tagger identity, timestamp, and message
echo "[1/3] Creating annotated tag ${TAG} at ${SHA} ..."
git tag -a "$TAG" "$SHA" -m "Release ${TAG}"
git push origin "$TAG"
TAG_PUSHED=true
echo "      Tag pushed."
echo ""

echo "[2/3] Creating GitHub release ..."
RELEASE_URL=$(gh release create "${RELEASE_ARGS[@]}")
echo "      Release URL: ${RELEASE_URL}"
echo ""

echo "[3/3] Done."
echo ""
echo "Release: ${RELEASE_URL}"
trap - EXIT
