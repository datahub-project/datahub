#!/usr/bin/env bash
# Run API or browser checks against the current worktree's running local Vite stack.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

case "${1:-}" in
    smoke|browser) ;;
    *) echo "Usage: $0 smoke|browser" >&2; exit 2 ;;
esac

# Use the live status to select this worktree, and the launcher's manifest for credentials and
# index names. Never infer ports or index prefixes from the worktree name or slot.
STATUS="$(scripts/dev/datahub-dev.sh status)"
TEST_ENV="$(python3 - "$STATUS" <<'PY'
import json
import pathlib
import shlex
import sys

status = json.loads(sys.argv[1])
assert status["mode"] == "local" and status["ready"], status
assert status["services"]["frontend"]["healthy"], status
stack = json.loads((pathlib.Path(status["data_dir"]) / "stack.json").read_text())
instance = stack["default"]
values = {
    "DATAHUB_GMS_URL": stack["instances"][instance],
    "DATAHUB_GMS_TOKEN": stack["adminTokens"][instance],
    "DATAHUB_FRONTEND_URL": f'http://localhost:{stack["frontendPort"]}',
    "ELASTICSEARCH_URL": f'http://localhost:{stack["opensearchPort"]}',
    "INDEX_PREFIX": stack["indexPrefixes"][instance],
    "ELASTICSEARCH_INDEX": f'{stack["indexPrefixes"][instance]}_datahub_usage_event',
}
for key, value in values.items():
    print(f"export {key}={shlex.quote(value)}")
PY
)"
eval "$TEST_ENV"

export ADMIN_USERNAME=datahub ADMIN_PASSWORD=datahub DATAHUB_TELEMETRY_ENABLED=false

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
        export BASE_URL="$DATAHUB_FRONTEND_URL" \
            DATAHUB_ADMIN_USERNAME="$ADMIN_USERNAME" DATAHUB_ADMIN_PASSWORD="$ADMIN_PASSWORD"
        login_test() {
            node_modules/.bin/playwright test --config=playwright.no-webserver.config.ts \
                tests/auth/login.spec.ts --grep 'logs in successfully with valid credentials' "$@"
        }
        # On a cold Vite cache the dependency scan cannot resolve the template-string theme import
        # in useSetAppTheme.tsx, so Vite discovers dependencies while the first page loads and
        # reloads it several times, outlasting the login helper's 30s wait. Repeat until a login
        # passes so the checked run measures a warm dev server.
        for attempt in 1 2 3 4; do
            echo "Warm-up login attempt $attempt/4"
            if login_test --output="test-results/warmup-$attempt"; then
                echo "Warm-up login passed on attempt $attempt/4"
                break
            fi
            echo "Warm-up login attempt $attempt/4 failed"
            if [[ "$attempt" == 4 ]]; then
                echo "Warm-up login did not converge after 4 attempts" >&2
                exit 1
            fi
        done
        login_test --trace=on
        ;;
esac
