#!/usr/bin/env bash
# compare-upstream.sh
#
# Compares acryldata/datahub (origin/master) against datahub-project/datahub (upstream).
# Outputs a structured summary of:
#   - OSS commits not yet merged into the fork
#   - Fork-only (acryl-specific) commits since the last sync
#   - Files changed in metadata-ingestion / datahub-actions by each group
#
# Usage:
#   .agent-skills/oss-release/scripts/compare-upstream.sh
#
# Prerequisites:
#   - git, with origin pointing to acryldata/datahub
#   - Network access to github.com

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "$REPO_ROOT"

for cmd in git gh python3; do
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

OSS_REMOTE="datahub-project"
OSS_URL="https://github.com/datahub-project/datahub.git"

echo "=== Upstream Diff Analysis ==="
echo ""

echo "Fetching origin master + tags ..."
if ! git fetch origin master --quiet; then
    echo "Error: 'git fetch origin master' failed. Check network and remote config." >&2
    exit 1
fi
# Delegate to safe-fetch-tags.sh — it suppresses noise from legacy 3-segment tag
# clobbers (irrelevant to current 4-segment release line) and only warns on real failures.
"$SCRIPT_DIR/safe-fetch-tags.sh" || true

# Reuse an existing remote that already points at the upstream URL
# (e.g. "upstream") rather than adding a duplicate "datahub-project" remote.
EXISTING_REMOTE=""
while IFS=$'\t' read -r name url_type_pair; do
    url="${url_type_pair% *}"
    type="${url_type_pair##* }"
    [ "$type" = "(fetch)" ] || continue
    case "$url" in
        *datahub-project/datahub|*datahub-project/datahub.git)
            EXISTING_REMOTE="$name"
            break
            ;;
    esac
done < <(git remote -v)
if [ -n "$EXISTING_REMOTE" ]; then
    OSS_REMOTE="$EXISTING_REMOTE"
    echo "Reusing existing remote '$OSS_REMOTE' → $OSS_URL"
elif ! git remote get-url "$OSS_REMOTE" &>/dev/null; then
    echo "Adding remote '$OSS_REMOTE' → $OSS_URL"
    git remote add "$OSS_REMOTE" "$OSS_URL"
fi
echo "Fetching $OSS_REMOTE/master ..."
if ! git fetch "$OSS_REMOTE" master --quiet; then
    echo "Error: 'git fetch $OSS_REMOTE master' failed." >&2
    echo "  Remote URL: $(git remote get-url "$OSS_REMOTE" 2>/dev/null || echo 'unknown')" >&2
    exit 1
fi

MERGE_BASE=$(git merge-base "$OSS_REMOTE/master" origin/master)
echo "Merge base: ${MERGE_BASE:0:10}"
echo ""

OSS_AHEAD=$(git rev-list "$MERGE_BASE..$OSS_REMOTE/master" --count)
FORK_AHEAD=$(git rev-list "$MERGE_BASE..origin/master" --count)
echo "Gap: OSS is ${OSS_AHEAD} commit(s) ahead | Fork is ${FORK_AHEAD} commit(s) ahead"
echo ""

echo "--- OSS commits not in fork (${OSS_AHEAD}) ---"
if [ "$OSS_AHEAD" -gt 0 ]; then
    git log "$MERGE_BASE..$OSS_REMOTE/master" \
        --pretty=format:"  %h %s" \
        --no-merges \
        -- metadata-ingestion/ datahub-actions/ 2>/dev/null | head -40
    echo ""
    echo "  File stats (metadata-ingestion + datahub-actions):"
    git diff --stat "$MERGE_BASE..$OSS_REMOTE/master" \
        -- metadata-ingestion/ datahub-actions/ 2>/dev/null | tail -5
else
    echo "  (none — fork is fully synced)"
fi
echo ""

echo "--- Fork-only commits since sync (${FORK_AHEAD}, non-merge) ---"
if [ "$FORK_AHEAD" -gt 0 ]; then
    git log "$MERGE_BASE..origin/master" \
        --pretty=format:"  %h %s" \
        --no-merges \
        -- metadata-ingestion/ datahub-actions/ 2>/dev/null | head -40
    echo ""
    echo "  File stats (metadata-ingestion + datahub-actions):"
    git diff --stat "$MERGE_BASE..origin/master" \
        -- metadata-ingestion/ datahub-actions/ 2>/dev/null | tail -5
else
    echo "  (none)"
fi
echo ""
echo "=== Done ==="
