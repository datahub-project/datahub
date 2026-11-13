#!/usr/bin/env bash
# Plan the pytest smoke batches for a PR's smart selection: the "Plan pytest
# batches" step of docker-unified.yml's smoke_test_matrix job.
#
# Reads SMOKE_SMART_TAGS and the planner's SMOKE_* settings from the step env,
# writes `selection` and `smoke_tier` to $GITHUB_OUTPUT, and dumps the plan to
# $SMOKE_DUMP_BATCH_PLAN. selection.py names the selection; SMOKE_TIER is only
# the marker set it selects from.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

selection="$(python3 .github/smart-testing/selection.py)"
if [[ "$selection" == p0 ]]; then
  SMOKE_TIER=p0
else
  SMOKE_TIER=full
fi
export SMOKE_TIER
{
  echo "selection=$selection"
  echo "smoke_tier=$SMOKE_TIER"
} >> "$GITHUB_OUTPUT"

./smoke-test/smoke.sh

SELECTION=$selection jq -r '"Planned \(.batch_count) batch(es) for \(env.SELECTION) selection: \(.test_count) of \(.candidate_test_count) \(env.SMOKE_TIER)-tier tests; predicted long pole \(.max_predicted_wall | floor)s (target \(.target_wall | floor)s, slowest scope \(.slowest_scope_wall | floor)s)"' \
  "$SMOKE_DUMP_BATCH_PLAN" | tee -a "$GITHUB_STEP_SUMMARY"
