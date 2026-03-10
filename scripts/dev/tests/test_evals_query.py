#!/usr/bin/env python3
"""
Pillar 4 — Query: agent eval suite for DataHub Common Operations CLI.

Tests that agents correctly use `datahub init` and `datahub graphql` rather
than ad-hoc alternatives (raw curl, manual config files, source-grepping).

Usage:
    python3 scripts/dev/tests/test_evals_query.py              # all evals
    python3 scripts/dev/tests/test_evals_query.py --eval 11    # specific eval
    python3 scripts/dev/tests/test_evals_query.py --runs 3     # repeat N times
    python3 scripts/dev/tests/test_evals_query.py --json       # JSON output
    python3 scripts/dev/tests/test_evals_query.py --dry-run    # print prompts only
"""

import sys
from typing import Dict, Tuple

from _eval_lib import (
    Eval,
    SessionOutput,
    _bash,
    _fmt,
    _read,
    _text,
    make_main,
)

# ---------------------------------------------------------------------------
# Graders
# ---------------------------------------------------------------------------


def _grade_init(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `datahub init`": _bash(s, r"datahub\s+init"),
        "did NOT manually write ~/.datahubenv": not _bash(
            s, r"(echo|cat|>>|>|Write|Edit).*datahubenv"
        ),
        "did NOT just export env vars without calling init": not (
            _bash(s, r"export\s+DATAHUB_GMS") and not _bash(s, r"datahub\s+init")
        ),
    }
    score = sum(checks.values()) / len(checks)
    passed = checks["used `datahub init`"]
    return passed, score, checks, _fmt(checks)


def _grade_graphql_discovery(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `datahub graphql --list-operations`": _bash(
            s, r"datahub\s+graphql.*--list-operations"
        ),
        "used `--format json` for machine-readable output": _bash(
            s, r"datahub\s+graphql.*--format\s+json|--format\s+json.*datahub\s+graphql"
        ),
        "did NOT grep/find source .graphql files": not _bash(
            s, r"grep.*\.graphql|find.*\.graphql|rg.*\.graphql"
        ),
        "did NOT curl introspection endpoint manually": not _bash(
            s, r"curl.*__schema|curl.*introspect"
        ),
    }
    score = sum(checks.values()) / len(checks)
    passed = checks["used `datahub graphql --list-operations`"]
    return passed, score, checks, _fmt(checks)


def _grade_graphql_mutation_dry_run(
    s: SessionOutput,
) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `datahub graphql --query` or `--operation`": _bash(
            s, r"datahub\s+graphql.*--(query|operation)"
        ),
        "used `--dry-run` before the live mutation": _bash(s, r"--dry-run"),
        "used `--format json`": _bash(s, r"--format\s+json"),
        "did NOT use raw curl to hit /api/v2/graphql": not _bash(
            s, r"curl.*graphql|curl.*api/v2"
        ),
    }
    score = sum(checks.values()) / len(checks)
    # Dry-run discipline is the core check here — it's the whole point of this eval
    passed = (
        checks["used `datahub graphql --query` or `--operation`"]
        and checks["used `--dry-run` before the live mutation"]
    )
    return passed, score, checks, _fmt(checks)


# ---------------------------------------------------------------------------
# Eval definitions
# ---------------------------------------------------------------------------

DEFINED_EVALS = [
    Eval(
        id=11,
        name="init: use `datahub init`, not manual config or env vars",
        prompt=(
            "Configure the datahub CLI to connect to my local DataHub instance "
            "(running at http://localhost:8080 with username 'datahub' and password 'datahub')."
        ),
        grade=_grade_init,
    ),
    Eval(
        id=12,
        name="graphql discovery: use --list-operations, not source grep or curl",
        prompt=(
            "What GraphQL operations are available on my local DataHub instance? "
            "Give me a machine-readable list."
        ),
        grade=_grade_graphql_discovery,
    ),
    Eval(
        id=13,
        name="graphql mutation: --dry-run before executing a mutation with variables",
        prompt=(
            "Use the DataHub graphql CLI to add the tag `urn:li:tag:PII` to the dataset "
            "`urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.mytable,PROD)`. "
            "Verify the request before executing it."
        ),
        grade=_grade_graphql_mutation_dry_run,
    ),
]

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

main = make_main(
    DEFINED_EVALS,
    description="Pillar 4 — Query: agent evals for DataHub Common Operations CLI",
)

if __name__ == "__main__":
    sys.exit(main())
