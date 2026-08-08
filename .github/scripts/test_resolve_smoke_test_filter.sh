#!/bin/bash
# Scenario table for resolve_smoke_test_filter.sh.
#
# The risk this guards is one-directional: narrowing the suite when the diff
# reaches beyond smoke tests, or when it touches shared machinery whose blast
# radius is not knowable from the path. Every case below that expects "" is
# asserting the full suite still runs.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNDER_TEST="${SCRIPT_DIR}/resolve_smoke_test_filter.sh"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

passed=0
failed=0

# check <name> <expected modules, comma separated> <changed files json>
check() {
  local name="$1" expected="$2" changed="$3"
  local out="${WORK_DIR}/out" summary="${WORK_DIR}/summary"
  : >"${out}"
  : >"${summary}"

  GITHUB_OUTPUT="${out}" GITHUB_STEP_SUMMARY="${summary}" \
    CHANGED_FILES="${changed}" "${UNDER_TEST}" >/dev/null 2>&1

  local actual
  actual=$(sed -n '/^smoke_test_modules<</,/^SMOKE_TEST_MODULES_EOF$/p' "${out}" |
    sed '1d;$d' | paste -sd, -)

  if [[ "${actual}" == "${expected}" ]]; then
    passed=$((passed + 1))
    printf '  ok    %s\n' "${name}"
    return
  fi
  failed=$((failed + 1))
  printf '  FAIL  %s\n        expected: %s\n        actual:   %s\n' \
    "${name}" "${expected:-<full suite>}" "${actual:-<full suite>}"
}

echo "narrows to the touched modules"
check "one modified test" "tests/policies/test_policies.py" \
  '["smoke-test/tests/policies/test_policies.py"]'
check "two modified tests" "tests/a/test_one.py,tests/b/two_test.py" \
  '["smoke-test/tests/a/test_one.py","smoke-test/tests/b/two_test.py"]'
check "test at the smoke-test root" "test_system_info.py" \
  '["smoke-test/test_system_info.py"]'

echo "runs the full suite when product code is involved"
check "feature PR that also edits a test" "" \
  '["metadata-io/src/main/java/A.java","smoke-test/tests/a/test_one.py"]'
check "frontend PR that also edits a test" "" \
  '["datahub-web-react/src/App.tsx","smoke-test/tests/a/test_one.py"]'
check "ingestion change alone" "" \
  '["metadata-ingestion/src/datahub/x.py"]'

echo "runs the full suite for shared smoke-test machinery"
check "root conftest" "" '["smoke-test/conftest.py"]'
check "nested conftest" "" '["smoke-test/tests/cypress/conftest.py"]'
check "tests/utils.py" "" '["smoke-test/tests/utils.py"]'
check "tests/utilities" "" '["smoke-test/tests/utilities/concurrent_test_runner.py"]'
check "consistency_utils" "" '["smoke-test/tests/consistency_utils.py"]'
check "a feature helper module" "" '["smoke-test/tests/knowledge/document_helpers.py"]'
check "fixture json" "" '["smoke-test/tests/policies/data.json"]'
check "requirements" "" '["smoke-test/requirements.txt"]'
check "the runner script" "" '["smoke-test/smoke.sh"]'
check "one test plus one helper" "" \
  '["smoke-test/tests/a/test_one.py","smoke-test/tests/utils.py"]'

echo "fails safe"
check "empty list" "" '[]'
check "unreadable list" "" 'not-json'

echo
echo "${passed} passed, ${failed} failed"
[[ "${failed}" -eq 0 ]]
