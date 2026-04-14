#!/usr/bin/env python3
"""
Pillar 3 — Operate: agent eval suite for DataHub dev environment operations.

Tests that agents correctly use datahub-dev.sh for env lifecycle, module
rebuilds, recovery escalation, feature flags, and setup commands.

Usage:
    python3 scripts/dev/tests/test_evals_operate.py              # all evals
    python3 scripts/dev/tests/test_evals_operate.py --eval 1,3   # specific evals
    python3 scripts/dev/tests/test_evals_operate.py --runs 3     # repeat N times
    python3 scripts/dev/tests/test_evals_operate.py --json       # JSON output
    python3 scripts/dev/tests/test_evals_operate.py --dry-run    # print prompts only
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

# Shared prompt for evals 1 and 5 (same task, different grading lens)
_ENV_LIFECYCLE_PROMPT = (
    "Enable DEV_TOOLING_ENABLED for the DataHub containers and confirm "
    "it is active in the running server."
)


def _grade_env_lifecycle(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "ran `env set DEV_TOOLING_ENABLED`": _bash(s, r"env\s+set\s+DEV_TOOLING_ENABLED"),
        "ran `env restart`": _bash(s, r"env\s+restart"),
        "verified via `env list` or saw pending_restart:false": (
            _bash(s, r"env\s+list") or _text(s, r"pending_restart")
        ),
        "did NOT stop after env set without restarting": not (
            _bash(s, r"env\s+set") and not _bash(s, r"env\s+restart")
        ),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 0.75, score, checks, _fmt(checks)


def _grade_module_mapping(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "identified datahub-gms as target": (
            _text(s, r"datahub.?gms") or _bash(s, r"datahub.?gms")
        ),
        "used rebuild (not reset/nuke)": (
            _bash(s, r"\srebuild\b") and not _bash(s, r"\b(reset|nuke)\b")
        ),
        "did not unnecessarily rebuild all containers": not _bash(s, r"nuke|docker\s+rm"),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 0.67, score, checks, _fmt(checks)


def _grade_recovery(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    has_keep_data = _text(s, r"nuke\s+--keep-data|--keep-data") or _bash(
        s, r"nuke.*--keep-data"
    )
    checks: Dict[str, bool] = {
        "suggested `nuke --keep-data`": has_keep_data,
        "did NOT suggest plain `nuke` (data loss) as first step": not (
            _text(s, r"\bnuke\b") and not _text(s, r"--keep-data")
        ),
        "did NOT re-suggest `reset`": not _text(s, r"(try|run|use)\s+reset"),
        "explained data-safety reasoning": _text(s, r"keep.data|data.loss|volumes|escalat"),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 0.75, score, checks, _fmt(checks)


def _grade_agent_mode(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "set AGENT_MODE=1": _bash(s, r"AGENT_MODE=1"),
        "referenced test-report.json": (
            _bash(s, r"test-report\.json") or _text(s, r"test-report\.json")
        ),
        "did NOT manually grep/awk pytest stdout": not (
            _bash(s, r"grep.*(PASSED|FAILED)|awk.*pytest|sed.*PASSED")
            and not _bash(s, r"AGENT_MODE")
        ),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 0.67, score, checks, _fmt(checks)


def _grade_proactive_read(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    # PASS = agent proactively read the docs  → hypothesis falsified
    # FAIL = agent flew blind without reading → hypothesis confirmed
    checks: Dict[str, bool] = {
        "read AGENTS.md": _read(s, "AGENTS.md"),
        "read any file in scripts/dev/": _read(s, "scripts/dev"),
    }
    any_read = any(checks.values())
    notes = _fmt(checks)
    notes += f"\n  files_read: {s.files_read or '(none)'}"
    return any_read, 1.0 if any_read else 0.0, checks, notes


def _grade_flag_lifecycle(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `env set SHOW_BROWSE_V2`": _bash(s, r"env\s+set\s+SHOW_BROWSE_V2"),
        "used `env restart`": _bash(s, r"env\s+restart"),
        "did NOT try to POST/PATCH a flag API": not _bash(
            s,
            r"curl.*(POST|PATCH).*flag",
            r"requests\.(post|patch).*flag",
        ),
        "did NOT treat `flag list/get` as writable": not _text(
            s, r"flag\s+(set|write|update|toggle)"
        ),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 0.75, score, checks, _fmt(checks)


def _grade_setup(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `datahub-dev.sh setup` or `datahub_dev.py setup`": _bash(
            s, r"datahub.dev\S*\s+setup"
        ),
        "did NOT use raw `./gradlew :metadata-ingestion:installDev`": not _bash(
            s, r"gradlew\s+:metadata-ingestion:installDev"
        ),
        "did NOT manually activate a venv or pip install": not _bash(
            s, r"source\s+.*/activate|pip\s+install"
        ),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 0.67, score, checks, _fmt(checks)


def _grade_frontend_start(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `datahub-dev.sh frontend` or `datahub_dev.py frontend`": _bash(
            s, r"datahub.dev\S*\s+frontend"
        ),
        "did NOT use raw `yarn start` or `cd datahub-web-react`": not _bash(
            s, r"yarn\s+start|cd\s+datahub-web-react"
        ),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 1.0, score, checks, _fmt(checks)


def _grade_setup_frontend(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `datahub-dev.sh setup frontend`": _bash(
            s, r"datahub.dev\S*\s+setup\s+frontend"
        ),
        "did NOT use raw `yarn install`": not _bash(s, r"yarn\s+install"),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 1.0, score, checks, _fmt(checks)


def _grade_env_set_generic(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `datahub-dev env set`": _bash(s, r"datahub.dev\S*\s+env\s+set"),
        "did NOT manually edit a .env file": not _bash(
            s, r"(echo|cat|>>|>)\s+.*\.env|Edit.*\.env|Write.*\.env"
        ),
        "did NOT use docker compose or export": not _bash(
            s, r"docker\s+compose.*-e|export\s+ELASTICSEARCH"
        ),
    }
    score = sum(checks.values()) / len(checks)
    passed = checks["used `datahub-dev env set`"]
    notes = _fmt(checks)
    if s.files_read:
        notes += f"\n  files_read: {s.files_read}"
    return passed, score, checks, notes


# ---------------------------------------------------------------------------
# Eval definitions
# ---------------------------------------------------------------------------

DEFINED_EVALS = [
    Eval(
        id=1,
        name="env lifecycle: set → restart → verify pending_restart:false",
        prompt=_ENV_LIFECYCLE_PROMPT,
        grade=_grade_env_lifecycle,
    ),
    Eval(
        id=2,
        name="module mapping: metadata-io/ change → rebuild datahub-gms only",
        prompt=(
            "I just changed code in metadata-io/. "
            "What is the fastest way to get it running in my local DataHub stack? "
            "Please do it."
        ),
        grade=_grade_module_mapping,
    ),
    Eval(
        id=3,
        name="recovery escalation: restart loop → nuke --keep-data, not nuke",
        prompt=(
            "GMS is in a restart loop. I already tried `datahub-dev.sh reset` and it "
            "did not fix it. What should I do next? Explain your reasoning before acting."
        ),
        grade=_grade_recovery,
    ),
    Eval(
        id=4,
        name="structured test output: AGENT_MODE=1 → test-report.json",
        prompt=(
            "Run smoke-test/tests/test_system_info.py and give me a "
            "machine-readable summary of what passed and failed."
        ),
        grade=_grade_agent_mode,
    ),
    Eval(
        id=5,
        name="meta: did agent proactively read AGENTS.md or scripts/dev/?",
        prompt=_ENV_LIFECYCLE_PROMPT,  # same task as eval 1
        grade=_grade_proactive_read,
        reuse_session_from=1,  # grade the eval 1 session, no extra API call
    ),
    Eval(
        id=6,
        name="flag lifecycle: toggle flag → env set + env restart, not flag API",
        prompt="Toggle the showBrowseV2 feature flag on in my local DataHub stack.",
        grade=_grade_flag_lifecycle,
    ),
    Eval(
        id=7,
        name="setup: use datahub-dev setup, not raw gradlew installDev",
        prompt=(
            "Set up the Python dev environment for DataHub ingestion "
            "so I can run the datahub CLI locally."
        ),
        grade=_grade_setup,
    ),
    Eval(
        id=8,
        name="frontend: use datahub-dev frontend, not raw yarn start",
        prompt=(
            "Start the DataHub frontend dev server with hot-reload "
            "so I can iterate on React changes."
        ),
        grade=_grade_frontend_start,
    ),
    Eval(
        id=9,
        name="setup frontend: use datahub-dev setup frontend, not raw yarn install",
        prompt=(
            "What is the correct command to install the frontend dependencies "
            "for DataHub so I can work on the React app? "
            "Show me the exact command, then run it."
        ),
        grade=_grade_setup_frontend,
    ),
    Eval(
        id=10,
        name="env set generic: use datahub-dev env set, not manual .env editing",
        prompt=(
            "Set ELASTICSEARCH_USE_SSL=true for the DataHub containers "
            "so they connect to Elasticsearch over TLS."
        ),
        grade=_grade_env_set_generic,
    ),
]

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

main = make_main(
    DEFINED_EVALS,
    description="Pillar 3 — Operate: agent evals for DataHub dev environment operations",
    meta_eval_id=5,
)

if __name__ == "__main__":
    sys.exit(main())
