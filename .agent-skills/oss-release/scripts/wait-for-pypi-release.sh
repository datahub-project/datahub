#!/usr/bin/env bash
# wait-for-pypi-release.sh <VERSION_TAG>
#
# Reports the state of the `pypi-release metadata-ingestion` workflow for a
# given release tag. The connector-tests workflow in acryldata/connector-tests
# does `pip install acryl-datahub==<version>` as its first step, so dispatching
# it before pypi has the new wheel produces a false-negative test run.
#
# Settle window: the workflow reporting `success` only means `twine upload`
# returned — pypi's index and its CDN edges converge a little later. Dispatching
# inside that window is the recurring release-race failure:
#
#   error: No solution found when resolving dependencies
#   (uv pip install 'acryl-datahub[testing-utils,bigquery,...]==<version>')
#
# So a successful run is not enough on its own: at least PYPI_SETTLE_SECONDS
# (default 120) must also have elapsed since the run completed. This is a
# probabilistic mitigation for index-propagation lag, not a hard guarantee —
# see the note at the bottom of this comment.
#
# The window is derived from the run's own `updatedAt` timestamp, so the script
# stays stateless: no local state file, nothing to keep in sync, and the caller
# can poll from any machine. It also adds no network calls beyond the single
# `gh run list` query the script already made.
#
# This script does NOT block — it's stateless. The caller polls by re-invoking
# (via /loop or manually) until it returns 0.
#
# Usage:
#   .agent-skills/oss-release/scripts/wait-for-pypi-release.sh v1.5.0.14rc1
#
# Environment:
#   PYPI_SETTLE_SECONDS  (default: 120) — minimum seconds between the
#       pypi-release run completing and "safe to dispatch". Set to 0 to disable
#       the wait entirely (not recommended; that restores the old racy behavior).
#
# Exit codes:
#   0 — pypi-release metadata-ingestion completed successfully AND the settle
#       window has elapsed; safe to dispatch connector-tests.
#   2 — no run found yet (release just created, workflow hasn't scheduled). Wait
#       ~30s and retry.
#   3 — run is queued or in progress. Wait a minute or two and retry (typical
#       end-to-end: 5-10 min).
#   4 — run completed with a non-success conclusion (failure, cancelled,
#       timed_out, etc.). Do NOT dispatch connector-tests — the wheel isn't on
#       pypi. Investigate the failure.
#   5 — run succeeded but the settle window has not elapsed yet. Distinct from 3
#       on purpose: nothing is running any more, you just have to wait out the
#       index-propagation grace period. Retry after the printed remaining time.
#   1 — usage error.
#
# Even at exit 0 a connector-test job can still lose the race occasionally. If
# one fails with `No solution found when resolving dependencies` shortly after a
# release, RE-RUN the failed connector-test jobs — do not re-cut the RC and do
# not treat it as a connector regression. See known-flaky-tests.md.

set -uo pipefail

TAG="${1:-}"
if [ -z "$TAG" ]; then
    echo "Usage: $0 <tag>  (e.g. v1.5.0.14rc1)" >&2
    exit 1
fi

REPO="acryldata/datahub"
WORKFLOW_NAME="pypi-release metadata-ingestion"
SETTLE_SECONDS="${PYPI_SETTLE_SECONDS:-120}"

echo "=== pypi-release metadata-ingestion: ${TAG} ==="

# Fetch release-event runs; filter server-side for efficiency (see check-ci.sh
# comment on why client-side filtering ages out older tags quickly).
RUNS_JSON=$(gh run list --repo "$REPO" --event release --limit 60 \
              --json name,status,conclusion,url,headBranch,createdAt,updatedAt)
export RUNS_JSON TAG WORKFLOW_NAME SETTLE_SECONDS

python3 << 'PYEOF'
import json, os, sys
from datetime import datetime, timezone

runs = json.loads(os.environ["RUNS_JSON"])
tag = os.environ["TAG"]
wf = os.environ["WORKFLOW_NAME"]
try:
    settle = max(0, int(float(os.environ["SETTLE_SECONDS"])))
except ValueError:
    print(f"  Invalid PYPI_SETTLE_SECONDS={os.environ['SETTLE_SECONDS']!r}; using 120.")
    settle = 120

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

if status != "completed":
    # status in (queued, in_progress, waiting, requested, pending, ...)
    print(f"  ⏳ pypi-release still running (status={status}). Typical end-to-end: 5-10 min.")
    print("     Poll again with the same command, or use /loop to auto-re-invoke every ~2 min.")
    sys.exit(3)

if conclusion != "success":
    print(f"  ✗ pypi-release ended with conclusion={conclusion}.")
    print("    Do NOT dispatch connector-tests — the wheel is NOT on pypi.")
    print("    Investigate the failing run and decide whether to re-cut the RC.")
    sys.exit(4)


def parse_ts(value):
    """GitHub returns RFC3339 UTC ('2026-09-17T12:34:56Z')."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


# `updatedAt` is the last state change of a completed run — i.e. when it
# finished. Fall back to createdAt if GitHub ever omits it.
finished = parse_ts(r.get("updatedAt")) or parse_ts(r.get("createdAt"))
if finished is None:
    print("  ! Could not parse the run's completion timestamp; assuming the")
    print("    settle window has NOT elapsed. Retry in a moment.")
    sys.exit(5)

# Negative elapsed means local clock skew, not a time machine — clamp it.
elapsed = max(0, int((datetime.now(timezone.utc) - finished).total_seconds()))
print(f"  Completed  : {finished.isoformat()} ({elapsed}s ago)")

if elapsed < settle:
    remaining = settle - elapsed
    print(f"  ⏳ pypi-release succeeded, but only {elapsed}s of the {settle}s settle window")
    print(f"     has elapsed. pypi's index and CDN edges need a moment after upload;")
    print(f"     dispatching now risks 'No solution found when resolving dependencies'.")
    print(f"     Retry in ~{remaining}s (override with PYPI_SETTLE_SECONDS).")
    sys.exit(5)

print(f"  ✓ pypi-release complete {elapsed}s ago (≥ {settle}s settle window).")
print("    Wheel should be on pypi. Safe to dispatch connector-tests.")
sys.exit(0)
PYEOF
