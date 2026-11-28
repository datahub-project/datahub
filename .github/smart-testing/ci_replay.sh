#!/usr/bin/env bash
# Replay the golden PRs through this branch's classifier: the replay step of
# the Smart Testing Replay workflow. It authenticates with Workload Identity
# Federation, the same as the classifier in docker-unified.yml, so no API key.
#
# Reads the ANTHROPIC_* federation settings (the token file comes from the
# claude-identity-token action) and GH_TOKEN from the step env, writes the
# report to the job summary, and exits 1 when replay_prs.py does (a
# coverage-risk or an error in most runs of a PR).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pip install --quiet -r "$here/requirements.txt"

report="${RUNNER_TEMP}/smart-testing-replay.txt"
status=0
# classify_pr.py appends to $GITHUB_OUTPUT; a replay must not set step outputs.
env -u GITHUB_OUTPUT python3 "$here/replay_prs.py" run --runs 3 | tee "$report" || status=$?
# The FAILED and WARNING sections come first, so the reason is the first thing
# on the summary page.
sections='^(FAILED|WARNING):'
flagged=false
if grep -q -E "$sections" "$report"; then flagged=true; fi
{
  echo "## Smart testing replay (golden PRs, 3 runs per PR)"
  echo
  if $flagged; then
    echo '```'
    sed -n -E "/$sections/,\$p" "$report" | grep -v '^::'
    echo '```'
    echo
    echo "<details><summary>All runs</summary>"
    echo
  fi
  echo '```'
  sed -E "/$sections/,\$d" "$report"
  echo '```'
  if $flagged; then echo "</details>"; fi
} >> "$GITHUB_STEP_SUMMARY"
exit "$status"
