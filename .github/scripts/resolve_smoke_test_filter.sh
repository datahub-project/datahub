#!/bin/bash
# When a pull request touches nothing but smoke test modules, run just those
# modules instead of the whole suite.
#
# Writes one GitHub output:
#
#   smoke_test_modules  Newline-separated test module paths to run. Empty means
#                       run everything, which is the default for anything this
#                       script is not certain about.
#
# Scope is deliberately narrow. A feature PR that also adds or edits smoke tests
# still runs the full battery -- that is the case where regressions matter and
# where the changed product code is what needs covering. This only fires when
# the diff is smoke tests and nothing else, which is the case where running 7
# batches of unrelated tests buys nothing.
#
# The tradeoff, stated plainly: smoke tests share one live DataHub instance, so
# running a subset means a test that leaks state (ingested entities it does not
# delete, a policy it does not restore) will not be caught by its neighbours
# here. Master runs the full suite on every merge, so it is caught there. The
# first line of defence is the isolation rules in smoke-test/CLAUDE.md and
# review enforcing them.

set -euo pipefail

# Shared machinery rather than a leaf test: conftest.py, tests/utils.py,
# tests/utilities/**, fixture JSON, requirements, the runner scripts. A change
# to any of these can alter the behaviour of tests it does not name, so the
# subset is no longer knowable and the full suite runs.
is_test_module() {
  local path="$1" base
  base="${path##*/}"
  [[ "${base}" == conftest.py ]] && return 1
  [[ "${base}" == test_*.py || "${base}" == *_test.py ]]
}

modules=()
filter=1

while IFS= read -r path; do
  [[ -n "${path}" ]] || continue
  # Anything outside smoke-test/ means product code changed: run the full suite.
  if [[ "${path}" != smoke-test/* ]] || ! is_test_module "${path}"; then
    filter=0
    break
  fi
  # conftest.py collects against paths relative to the smoke-test directory.
  modules+=("${path#smoke-test/}")
done < <(jq -r '.[]?' <<<"${CHANGED_FILES:-[]}" 2>/dev/null)

# No files at all means the list never arrived; run everything.
if ((${#modules[@]} == 0)); then
  filter=0
fi

{
  echo "smoke_test_modules<<SMOKE_TEST_MODULES_EOF"
  if ((filter)); then
    printf '%s\n' "${modules[@]}"
  fi
  echo "SMOKE_TEST_MODULES_EOF"
} >>"${GITHUB_OUTPUT}"

if ((filter)); then
  echo "Smoke-test-only diff: running ${#modules[@]} touched module(s) instead of the full suite"
  printf '  %s\n' "${modules[@]}"
  {
    echo "## Smoke test filter"
    echo ""
    echo "Diff touches smoke tests only, so the suite is narrowed to:"
    echo ""
    printf -- "-  \`%s\`\n" "${modules[@]}"
  } >>"${GITHUB_STEP_SUMMARY}"
else
  echo "Running the full smoke test suite"
fi
