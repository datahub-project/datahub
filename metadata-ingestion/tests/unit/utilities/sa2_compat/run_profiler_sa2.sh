#!/usr/bin/env bash
# Runs the SQLAlchemy profiler tests against SQLAlchemy 2.0 in an isolated venv.
# Dialect-agnostic: uses in-memory SQLite, so no connector packages are needed.
#
# Usage:
#   bash metadata-ingestion/tests/unit/utilities/sa2_compat/run_profiler_sa2.sh
#
# Strategy: mirrors run_combiner_sa2.sh — install the datahub package with its
# base (non-extras) deps, then upgrade SQLAlchemy to >=2.0 and add the handful
# of extra deps needed by the profiler test import chain.

set -euo pipefail

ING_DIR="$(cd "$(dirname "$0")/../../../.." && pwd)"   # metadata-ingestion/
PROFILER_TEST_DIR="$ING_DIR/tests/unit/sqlalchemy_profiler"
TMP_VENV_PARENT="$(mktemp -d)"
TMP_VENV="$TMP_VENV_PARENT/sa2-profiler-venv"

trap 'rm -rf "$TMP_VENV_PARENT"' EXIT

echo "=== SQLAlchemy 2.0 profiler test harness ==="
echo "ING_DIR          : $ING_DIR"
echo "PROFILER_TEST_DIR: $PROFILER_TEST_DIR"
echo "VENV             : $TMP_VENV"
echo ""

python3 -m venv "$TMP_VENV"
"$TMP_VENV/bin/pip" install -q --upgrade pip

# Step 1: install datahub base package (no extras).
echo "Installing datahub base dependencies..."
"$TMP_VENV/bin/pip" install -q -e "$ING_DIR"

# Step 2: upgrade SQLAlchemy to 2.0.
echo "Upgrading SQLAlchemy to 2.0..."
"$TMP_VENV/bin/pip" install -q "sqlalchemy>=2.0,<3" greenlet

# Step 3: add extra deps required by the profiler test import chain.
# sqlglot[c]==30.8.0 pinned: >=30.7.0 SIGSEGVs on LATERAL/explode over un-cataloged tables (ING-2868)
echo "Installing extra deps for profiler import chain..."
"$TMP_VENV/bin/pip" install -q \
    "acryl-datahub-classify==0.0.11" \
    "cachetools<6.0.0" \
    "sqlparse<0.6.0" \
    "sqlglot[c]==30.8.0" \
    "patchy==2.8.0" \
    "pytest>=8,<10"

# Print actual SA version so the output is easy to grep.
echo ""
"$TMP_VENV/bin/python" -c "import sqlalchemy; print('SQLAlchemy', sqlalchemy.__version__)"
echo ""

# Run the tests. We intentionally do NOT pass -x so we see all failures.
# --noconftest: skip the top-level conftest.py which needs docker helpers.
# --ignore test_adapters.py: imports connector packages (pyathena, etc.) not available in the
#   sqlite-only venv; those tests pass on SA 1.4 and are flagged for connector-revalidation.
echo "=== Running profiler test suite ==="
"$TMP_VENV/bin/pytest" "$PROFILER_TEST_DIR" \
    --ignore="$PROFILER_TEST_DIR/test_adapters.py" \
    -v --tb=short --noconftest

echo ""
echo "=== Harness complete ==="
