#!/usr/bin/env bash
# Run all shell tests in this directory.
# Each test*.sh exits 0 on pass, non-zero (= number of failed cases) on fail.
#
# Usage:
#   .agent-skills/oss-release/tests/run-all.sh
#   SKIP_NETWORK=1 .agent-skills/oss-release/tests/run-all.sh   # skip live-network tests

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOTAL_FAIL=0

for test in "$SCRIPT_DIR"/test-*.sh; do
    [ -f "$test" ] || continue
    echo ""
    echo "================================================================"
    echo "Running: $(basename "$test")"
    echo "================================================================"
    if "$test"; then
        :
    else
        rc=$?
        TOTAL_FAIL=$((TOTAL_FAIL + rc))
    fi
done

echo ""
echo "================================================================"
if [ "$TOTAL_FAIL" -eq 0 ]; then
    echo "ALL TESTS PASSED ✅"
else
    echo "TOTAL FAILURES: $TOTAL_FAIL ❌"
fi
echo "================================================================"
exit "$TOTAL_FAIL"
