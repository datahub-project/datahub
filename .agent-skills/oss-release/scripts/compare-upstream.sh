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

# Resolve repo root from the script's own location — works regardless of CWD
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "$REPO_ROOT"

# --- Prerequisites check ---
for cmd in git gh python3; do
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

OSS_REMOTE="datahub-project"
OSS_URL="https://github.com/datahub-project/datahub.git"
ORIGIN_BRANCH="origin/master"
OSS_BRANCH="${OSS_REMOTE}/master"

echo "=== OSS Upstream Diff Analysis ==="
echo ""

# Add remote if missing (idempotent)
if ! git remote get-url "$OSS_REMOTE" &>/dev/null; then
    echo "[setup] Adding remote: ${OSS_REMOTE} -> ${OSS_URL}"
    git remote add "$OSS_REMOTE" "$OSS_URL"
fi

# Fetch both
echo "[fetch] Fetching origin/master ..."
git fetch origin master --quiet

echo "[fetch] Fetching ${OSS_BRANCH} ..."
git fetch "$OSS_REMOTE" master --depth=200 --quiet

# Compute merge base
MERGE_BASE=$(git merge-base "$ORIGIN_BRANCH" "$OSS_BRANCH")
echo ""
echo "Merge base (last common commit): ${MERGE_BASE}"
echo ""

# OSS commits not in fork
OSS_AHEAD=$(git log --oneline "${MERGE_BASE}..${OSS_BRANCH}" | wc -l | tr -d ' ')
FORK_AHEAD=$(git log --oneline --no-merges "${MERGE_BASE}..${ORIGIN_BRANCH}" | wc -l | tr -d ' ')

echo "--- Sync Status ---"
echo "OSS commits ahead of fork : ${OSS_AHEAD}"
echo "Fork commits ahead of OSS : ${FORK_AHEAD} (non-merge)"
echo ""

if [ "$OSS_AHEAD" -gt 0 ]; then
    echo "--- OSS commits NOT yet in fork (${OSS_AHEAD}) ---"
    git log --oneline "${MERGE_BASE}..${OSS_BRANCH}"
    echo ""

    echo "--- OSS changes in metadata-ingestion / datahub-actions ---"
    git diff "${MERGE_BASE}..${OSS_BRANCH}" --stat \
        -- metadata-ingestion/ datahub-actions/ \
        2>/dev/null || echo "(none)"
    echo ""
else
    echo "Fork is fully synced with OSS — no upstream gap."
    echo ""
fi

if [ "$FORK_AHEAD" -gt 0 ]; then
    echo "--- Fork-only (acryl-specific) non-merge commits (${FORK_AHEAD}) ---"
    git log --oneline --no-merges "${MERGE_BASE}..${ORIGIN_BRANCH}"
    echo ""

    echo "--- Fork-only changes in metadata-ingestion / datahub-actions ---"
    FORK_INGESTION=$(git log --oneline --no-merges \
        "${MERGE_BASE}..${ORIGIN_BRANCH}" \
        -- metadata-ingestion/ datahub-actions/ 2>/dev/null | wc -l | tr -d ' ')
    echo "  Ingestion-touching fork commits: ${FORK_INGESTION}"
    if [ "$FORK_INGESTION" -gt 0 ]; then
        git log --oneline --no-merges \
            "${MERGE_BASE}..${ORIGIN_BRANCH}" \
            -- metadata-ingestion/ datahub-actions/
    fi
    echo ""
fi

echo "=== Done ==="
