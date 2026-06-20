#!/usr/bin/env bash
# Runs the SQLAlchemyQueryCombiner tests against SQLAlchemy 2.0 in an isolated venv.
# Dialect-agnostic: uses in-memory SQLite, so no connector packages are needed.
#
# Usage:
#   bash metadata-ingestion/tests/unit/utilities/sa2_compat/run_combiner_sa2.sh
#
# Strategy: install the datahub package with its base (non-extras) deps, then
# upgrade SQLAlchemy to >=2.0 and add the handful of extra deps needed by the
# combiner test's import chain (sqlglot, patchy, classify, etc.).  This approach
# is more reliable than --no-deps because it avoids chasing each missing transitive
# import individually.  The SA pin on the base install (sqlalchemy<2) is simply
# overridden by the subsequent upgrade step.
#
# --noconftest: the top-level tests/conftest.py pulls in time_machine, docker helpers,
# and graph mocks that are not needed for the combiner unit tests.

set -euo pipefail

ING_DIR="$(cd "$(dirname "$0")/../../../.." && pwd)"   # metadata-ingestion/
TEST_FILE="$ING_DIR/tests/unit/utilities/test_sqlalchemy_query_combiner.py"
TMP_VENV="$(mktemp -d)/sa2-venv"

echo "=== SQLAlchemy 2.0 combiner test harness ==="
echo "ING_DIR : $ING_DIR"
echo "TEST_FILE: $TEST_FILE"
echo "VENV    : $TMP_VENV"
echo ""

python3 -m venv "$TMP_VENV"
"$TMP_VENV/bin/pip" install -q --upgrade pip

# Step 1: install datahub base package (no extras) — picks up all framework_common deps.
# This installs sqlalchemy<2 initially; we upgrade it in step 2.
echo "Installing datahub base dependencies..."
"$TMP_VENV/bin/pip" install -q -e "$ING_DIR"

# Step 2: upgrade SQLAlchemy to 2.0 and add greenlet (not in base deps).
echo "Upgrading SQLAlchemy to 2.0..."
"$TMP_VENV/bin/pip" install -q "sqlalchemy>=2.0,<3" greenlet

# Step 3: add the extra deps that the combiner test's deep import chain requires
# but that are not in datahub's base install_requires:
#   - acryl-datahub-classify: imported at module level by sql_report -> classification_mixin
#   - cachetools: imported by ge_profiling_config -> configuration.common
#   - sqlparse: imported by sql_report -> sql_parsing_aggregator
#   - sqlglot+patchy: imported by sql_report -> sql_parsing._models
#   - pytest + time_machine: test runner deps (time_machine used by top-level conftest,
#     but we skip that via --noconftest; still needed for clean venv setup)
echo "Installing extra deps for combiner import chain..."
"$TMP_VENV/bin/pip" install -q \
    "acryl-datahub-classify==0.0.11" \
    "cachetools<6.0.0" \
    "sqlparse<0.6.0" \
    "sqlglot[c]==30.8.0" \
    "patchy==2.8.0" \
    pytest

# Print actual SA version so the output is easy to grep.
echo ""
"$TMP_VENV/bin/python" -c "import sqlalchemy; print('SQLAlchemy', sqlalchemy.__version__)"
echo ""

# Run the tests. We intentionally do NOT pass -x so we see all failures.
# --noconftest: skip the top-level conftest.py which needs time_machine and docker helpers.
echo "=== Running test suite ==="
"$TMP_VENV/bin/pytest" "$TEST_FILE" -v --tb=short --noconftest 2>&1 || true

echo ""
echo "=== Harness complete ==="
