#!/usr/bin/env bash
# check-ci.sh
#
# Polls GitHub Actions CI status for a given commit SHA on acryldata/datahub.
# Outputs a human-readable summary grouped by status, scoped to release-triggered runs.
#
# Usage:
#   .agent-skills/oss-release/scripts/check-ci.sh <commit-sha>
#
# Prerequisites:
#   - gh CLI authenticated
#   - python3 in PATH

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "$REPO_ROOT"

for cmd in gh python3; do
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

REPO="acryldata/datahub"
SHA="${1:-}"

if [ -z "$SHA" ]; then
    echo "Usage: $0 <commit-sha>"
    exit 1
fi

export SHORT_SHA="${SHA:0:10}"

echo "=== CI Status: ${SHORT_SHA} (${REPO}) ==="
echo ""

# Filter server-side to event=release so the limit applies to *release-triggered*
# runs, not any recent run. acryldata/datahub is busy enough that 60 mixed-event
# runs cover only ~9 hours of wall time, so a release older than that would
# otherwise age out of the window even though its release-triggered CI runs
# still exist on GitHub.
#
# Also: pass JSON via env var instead of pipeline. A `gh ... | python3 - <<EOF`
# heredoc takes over stdin (to deliver the program), so the upstream JSON never
# reaches `sys.stdin.read()`. Capturing to a variable and handing off via env
# sidesteps the collision and keeps pipefail useful for catching gh failures.
RUNS_JSON=$(gh run list --repo "$REPO" --event release --limit 60 \
              --json name,status,conclusion,url,headSha,event)
export RUNS_JSON

python3 << 'PYEOF'
import json, os, sys

runs = json.loads(os.environ["RUNS_JSON"])
sha_prefix = os.environ["SHORT_SHA"]

release_runs = [
    r for r in runs
    if r.get("event") == "release"
    and r.get("headSha", "").startswith(sha_prefix)
]

if not release_runs:
    print(f"No release-triggered runs found for commit {sha_prefix}.")
    print("(Runs may not have started yet, or the commit was not tagged.)")
    sys.exit(0)

success, running, queued, failed, other = [], [], [], [], []

for r in release_runs:
    s = r["status"]
    c = r.get("conclusion") or ""
    n = r["name"]
    u = r["url"]
    if s == "completed":
        if c == "success":
            success.append(n)
        elif c in ("failure", "timed_out", "startup_failure"):
            failed.append((n, c, u))
        else:
            other.append((n, c))
    elif s == "in_progress":
        running.append(n)
    else:
        queued.append(n)

total = len(release_runs)
done  = len(success) + len(failed) + len(other)

print(f"Progress: {done}/{total} complete")
print()
print(f"SUCCESS ({len(success)}):")
for n in success:
    print(f"  ✓ {n}")
print()
print(f"IN PROGRESS ({len(running)}):")
for n in running:
    print(f"  … {n}")
print()
print(f"QUEUED ({len(queued)}):")
for n in queued:
    print(f"  ⏳ {n}")
print()
if failed:
    print(f"FAILED ({len(failed)}):")
    for n, c, u in failed:
        print(f"  ✗ {n}  [{c}]")
        print(f"    {u}")
    print()
if other:
    print(f"OTHER ({len(other)}):")
    for n, c in other:
        print(f"  ? {n}  [{c}]")
    print()
PYEOF
