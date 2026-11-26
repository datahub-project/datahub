#!/usr/bin/env bash
# Run API or browser checks against the current worktree's running local Vite stack.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

case "${1:-}" in
    smoke|browser) ;;
    *) echo "Usage: $0 smoke|browser" >&2; exit 2 ;;
esac

scripts/dev/datahub-dev.sh status | python3 -c '
import json, sys
status = json.load(sys.stdin)
assert status["mode"] == "local" and status["ready"], status
assert status["services"]["frontend"]["healthy"], status
'
# Built from this worktree's stack.json; logs in as datahub/datahub, so no GMS token.
eval "$(scripts/dev/datahub-dev.sh shell-env --tests)"

case "$1" in
    smoke)
        cd smoke-test
        venv/bin/python -m pytest -o addopts="" -p no:cacheprovider -v --timeout=300 \
            --junitxml=build/local-stack-smoke.xml \
            tests/e2e/search/test_search_projection.py::TestSearchProjection::test_minimal_projection \
            tests/e2e/lineage/test_lineage_sdk.py::test_table_level_lineage \
            tests/e2e/authorization/test_self_entity_auth.py::test_user_can_edit_own_profile
        ;;
    browser)
        cd e2e-test/ui/playwright
        login_test() {
            node_modules/.bin/playwright test --config=playwright.no-webserver.config.ts \
                tests/auth/login.spec.ts --grep 'logs in successfully with valid credentials' "$@"
        }
        # Playwright empties its output directory at the start of every run, so keep the warm-up
        # output and the resource samples outside test-results/ where the checked run cannot
        # delete them. The workflow uploads this directory on failure.
        DIAG_DIR="$PWD/diagnostics"
        rm -rf "$DIAG_DIR"
        mkdir -p "$DIAG_DIR"
        sample_resources() {
            while true; do
                {
                    echo "--- $(date -u +%H:%M:%S) loadavg: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || uptime)"
                    free -m 2>/dev/null || true
                    cat /proc/pressure/cpu /proc/pressure/memory 2>/dev/null || true
                    ps -A -o pcpu=,rss=,args= | sort -rn | head -8 | cut -c1-160
                } >> "$DIAG_DIR/resources.log"
                sleep 2
            done
        }
        sample_resources &
        SAMPLER_PID=$!
        trap 'kill "$SAMPLER_PID" 2>/dev/null || true' EXIT

        # Vite compiles modules on first request, so the first login on a cold cache spends most of
        # its time compiling (about 30s on CI runners, against the login helper's 30s wait). One
        # untimed pass compiles them; the checked run then measures a warm dev server.
        login_test --trace=on --output="$DIAG_DIR/warmup" >"$DIAG_DIR/warmup.log" 2>&1 || echo "warm-up login did not finish; running the check anyway"
        login_test --trace=on
        ;;
esac
