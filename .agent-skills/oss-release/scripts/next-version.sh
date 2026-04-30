#!/usr/bin/env bash
# next-version.sh
#
# Determines the next release version from existing git tags.
# Ported from the release.sh monolith version calculation logic.
#
# Usage:
#   next-version.sh [rc|stable] [fourth|patch|minor|major]
#
# Defaults:
#   - release_type: rc
#   - bump_type:    fourth  (unless latest tag is already an RC, in which case bumps RC number)
#
# Output: a single line with the next version, e.g. "v1.5.0.8rc1"
#
# Prerequisites:
#   - git with tags fetched (run: git fetch origin --tags --quiet)
#   - gh CLI authenticated (used as fallback for release listing)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# REPO_ROOT can be overridden (e.g. by tests) to point at a sandbox repo. Defaults
# to the project root inferred from this script's install location.
REPO_ROOT="${REPO_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
cd "$REPO_ROOT"

RELEASE_TYPE="${1:-rc}"
BUMP_TYPE="${2:-fourth}"

if [[ "$RELEASE_TYPE" != "rc" && "$RELEASE_TYPE" != "stable" ]]; then
    echo "Error: release_type must be 'rc' or 'stable', got: $RELEASE_TYPE" >&2
    exit 1
fi
if [[ "$BUMP_TYPE" != "fourth" && "$BUMP_TYPE" != "patch" && "$BUMP_TYPE" != "minor" && "$BUMP_TYPE" != "major" ]]; then
    echo "Error: bump_type must be 'fourth', 'patch', 'minor', or 'major', got: $BUMP_TYPE" >&2
    exit 1
fi

# ── version helpers ───────────────────────────────────────────────────────────

# Produces a zero-padded sortable key; stable versions rank above RC of same base.
_version_sort_key() {
    local v="${1#v}"
    local major=0 minor=0 patch=0 fourth=0 rc=99999
    if   [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)rc([0-9]+)$ ]]; then
        major=${BASH_REMATCH[1]}; minor=${BASH_REMATCH[2]}; patch=${BASH_REMATCH[3]}
        fourth=${BASH_REMATCH[4]}; rc=${BASH_REMATCH[5]}
    elif [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        major=${BASH_REMATCH[1]}; minor=${BASH_REMATCH[2]}; patch=${BASH_REMATCH[3]}
        fourth=${BASH_REMATCH[4]}
    elif [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)rc([0-9]+)$ ]]; then
        major=${BASH_REMATCH[1]}; minor=${BASH_REMATCH[2]}; patch=${BASH_REMATCH[3]}; rc=${BASH_REMATCH[4]}
    elif [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)-rc([0-9]+)$ ]]; then
        major=${BASH_REMATCH[1]}; minor=${BASH_REMATCH[2]}; patch=${BASH_REMATCH[3]}; rc=${BASH_REMATCH[4]}
    elif [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        major=${BASH_REMATCH[1]}; minor=${BASH_REMATCH[2]}; patch=${BASH_REMATCH[3]}
    fi
    printf "%010d%010d%010d%010d%010d" "$major" "$minor" "$patch" "$fourth" "$rc"
}

_sort_tags() {
    while IFS= read -r tag; do
        [[ $tag =~ ^v[0-9] ]] && echo "$(_version_sort_key "$tag") $tag"
    done | sort -rn | awk '{print $2}'
}

_parse_version() {
    local v="${1#v}"
    if   [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)rc([0-9]+)$ ]]; then
        echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]} ${BASH_REMATCH[3]} ${BASH_REMATCH[4]} ${BASH_REMATCH[5]}"
    elif [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]} ${BASH_REMATCH[3]} ${BASH_REMATCH[4]} 0"
    elif [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)-rc([0-9]+)$ ]]; then
        echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]} ${BASH_REMATCH[3]} 0 ${BASH_REMATCH[4]}"
    elif [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)rc([0-9]+)$ ]]; then
        echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]} ${BASH_REMATCH[3]} 0 ${BASH_REMATCH[4]}"
    elif [[ $v =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]} ${BASH_REMATCH[3]} 0 0"
    else
        echo "Error: cannot parse version: $1" >&2; exit 1
    fi
}

_is_rc() { [[ "$1" =~ rc[0-9]+$ ]]; }

_get_latest_tag() {
    local tag
    tag=$(git tag -l 'v*' | _sort_tags | head -n 1)
    if [ -z "$tag" ] && command -v gh &>/dev/null && gh auth token &>/dev/null; then
        tag=$(gh release list --repo acryldata/datahub --limit 100 --json tagName \
            --jq '.[].tagName' 2>/dev/null | _sort_tags | head -n 1)
    fi
    echo "${tag:-v0.1.0.0}"
}

_get_latest_stable_tag() {
    local tag
    tag=$(git tag -l 'v*' | grep -v 'rc' | _sort_tags | head -n 1)
    if [ -z "$tag" ] && command -v gh &>/dev/null && gh auth token &>/dev/null; then
        tag=$(gh release list --repo acryldata/datahub --limit 100 --json tagName \
            --jq '.[].tagName' 2>/dev/null | grep -v 'rc' | _sort_tags | head -n 1)
    fi
    echo "${tag:-v0.1.0.0}"
}

# ── main logic ────────────────────────────────────────────────────────────────

"$SCRIPT_DIR/safe-fetch-tags.sh" || \
    echo "Warning: tag fetch failed — version may be based on stale local tags" >&2

LATEST=$(_get_latest_tag)

# Auto-mode: if no explicit args and latest tag is already an RC, just bump the RC number
if [ $# -eq 0 ] && _is_rc "$LATEST"; then
    read -r major minor patch fourth rc <<< "$(_parse_version "$LATEST")"
    rc=$((rc + 1))
    if [ "$fourth" -eq 0 ]; then
        echo "v${major}.${minor}.${patch}rc${rc}"
    else
        echo "v${major}.${minor}.${patch}.${fourth}rc${rc}"
    fi
    exit 0
fi

# Stable-promotion shortcut: `stable` with no explicit bump and the latest tag
# is an RC means "finalize this RC" — strip the rcN suffix, don't bump.
# This is what `finish` uses to derive the stable version from LATEST_RC.
if [ "$RELEASE_TYPE" = "stable" ] && [ $# -le 1 ] && _is_rc "$LATEST"; then
    read -r major minor patch fourth rc <<< "$(_parse_version "$LATEST")"
    if [ "$fourth" -eq 0 ]; then
        echo "v${major}.${minor}.${patch}"
    else
        echo "v${major}.${minor}.${patch}.${fourth}"
    fi
    exit 0
fi

# For stable releases with an explicit bump (or when no RC exists), base the
# bump on the latest STABLE tag (skip RCs).
if [ "$RELEASE_TYPE" = "stable" ]; then
    BASE=$(_get_latest_stable_tag)
else
    BASE="$LATEST"
fi

read -r major minor patch fourth rc <<< "$(_parse_version "$BASE")"

if [ "$RELEASE_TYPE" = "rc" ]; then
    if [ "$rc" -gt 0 ]; then
        # Base is already an RC — increment RC number, no version component change
        rc=$((rc + 1))
    else
        # New RC cycle — bump according to BUMP_TYPE
        case "$BUMP_TYPE" in
            major) major=$((major + 1)); minor=0; patch=0; fourth=0 ;;
            minor) minor=$((minor + 1)); patch=0; fourth=0 ;;
            patch) patch=$((patch + 1)); fourth=0 ;;
            fourth) fourth=$((fourth + 1)) ;;
        esac
        rc=1
    fi
    if [ "$BUMP_TYPE" = "major" ]; then
        echo "v${major}.${minor}.${patch}rc${rc}"
    else
        echo "v${major}.${minor}.${patch}.${fourth}rc${rc}"
    fi
else
    # Stable release — same bump semantics as RC, just without the rcN suffix.
    # Keeps RC→stable correspondence (e.g. v2.0.0rc1 → v2.0.0, v1.6.0.0rc1 → v1.6.0.0).
    case "$BUMP_TYPE" in
        major) major=$((major + 1)); minor=0; patch=0; echo "v${major}.${minor}.${patch}" ;;
        minor) minor=$((minor + 1)); patch=0; fourth=0; echo "v${major}.${minor}.${patch}.${fourth}" ;;
        patch) patch=$((patch + 1)); fourth=0; echo "v${major}.${minor}.${patch}.${fourth}" ;;
        fourth) fourth=$((fourth + 1)); echo "v${major}.${minor}.${patch}.${fourth}" ;;
    esac
fi
