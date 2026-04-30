#!/usr/bin/env bash
# wait-for-pypi-release.sh <VERSION_TAG>
#
# Reports the state of the `pypi-release metadata-ingestion` workflow for a
# given release tag. The connector-tests workflow in acryldata/connector-tests
# does `pip install acryl-datahub==<version>` as its first step, so dispatching
# it before pypi has the new wheel produces a false-negative test run.
#
# This script does NOT block — it's stateless. The caller polls by re-invoking
# (via /loop or manually) until it returns 0.
#
# Usage:
#   .agent-skills/oss-release/scripts/wait-for-pypi-release.sh v1.5.0.14rc1
#
# Exit codes:
#   0 — pypi-release metadata-ingestion completed successfully; safe to dispatch
#       connector-tests.
#   2 — no run found yet (release just created, workflow hasn't scheduled). Wait
#       ~30s and retry.
#   3 — run is queued or in progress. Wait a minute or two and retry (typical
#       end-to-end: 5-10 min).
#   4 — run completed with a non-success conclusion (failure, cancelled,
#       timed_out, etc.). Do NOT dispatch connector-tests — the wheel isn't on
#       pypi. Investigate the failure.
#   1 — usage error.

set -uo pipefail

TAG="${1:-}"
if [ -z "$TAG" ]; then
    echo "Usage: $0 <tag>  (e.g. v1.5.0.14rc1)" >&2
    exit 1
fi

REPO="acryldata/datahub"
WORKFLOW_NAME="pypi-release metadata-ingestion"

echo "=== pypi-release metadata-ingestion: ${TAG} ==="

# Fetch release-event runs; filter server-side for efficiency (see check-ci.sh
# comment on why client-side filtering ages out older tags quickly).
RUNS_JSON=$(gh run list --repo "$REPO" --event release --limit 60 \
              --json name,status,conclusion,url,headBranch,createdAt)
export RUNS_JSON TAG WORKFLOW_NAME

python3 << 'PYEOF'
import json, os, sys

runs = json.loads(os.environ["RUNS_JSON"])
tag = os.environ["TAG"]
wf = os.environ["WORKFLOW_NAME"]

matching = [
    r for r in runs
    if r.get("headBranch") == tag and r.get("name") == wf
]

if not matching:
    print(f"  No '{wf}' run found for {tag} yet.")
    print(f"  The release event may not have fired yet. Wait ~30s and retry.")
    sys.exit(2)

# Most recent first (runs sorted by createdAt descending)
r = sorted(matching, key=lambda r: r.get("createdAt", ""), reverse=True)[0]
status     = r.get("status", "")
conclusion = r.get("conclusion") or ""
url        = r.get("url", "")

print(f"  Status     : {status}")
print(f"  Conclusion : {conclusion or '-'}")
print(f"  URL        : {url}")

if status == "completed":
    if conclusion == "success":
        print("  ✓ pypi-release complete — wheel is on pypi. Safe to dispatch connector-tests.")
        sys.exit(0)
    print(f"  ✗ pypi-release ended with conclusion={conclusion}.")
    print("    Do NOT dispatch connector-tests — the wheel is NOT on pypi.")
    print("    Investigate the failing run and decide whether to re-cut the RC.")
    sys.exit(4)

# status in (queued, in_progress, waiting, requested, pending, ...)
print(f"  ⏳ pypi-release still running (status={status}). Typical end-to-end: 5-10 min.")
print("     Poll again with the same command, or use /loop to auto-re-invoke every ~2 min.")
sys.exit(3)
PYEOF
