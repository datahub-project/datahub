#!/usr/bin/env python3
"""
Agent eval suite for DataHub dev tooling knowledge.

Tests the hypothesis: without @AGENTS.md imported in CLAUDE.md, Claude Code
will fail tasks that require knowledge outside the TLDR — env lifecycle,
module-to-container mapping, recovery escalation, AGENT_MODE.

Each eval runs `claude -p "<prompt>"` in a fresh session and grades the
observable tool calls (Bash commands, files read) plus response text.
Evals 1 and 5 share a session (same prompt, different grading lens) to
avoid duplicate API spend.

Usage:
    python3 scripts/dev/evals/agent_evals.py              # all evals
    python3 scripts/dev/evals/agent_evals.py --eval 1,3   # specific evals
    python3 scripts/dev/evals/agent_evals.py --runs 3     # repeat N times
    python3 scripts/dev/evals/agent_evals.py --json       # JSON output
    python3 scripts/dev/evals/agent_evals.py --dry-run    # print prompts, no API calls
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class SessionOutput:
    bash_commands: List[str] = field(default_factory=list)
    files_read: List[str] = field(default_factory=list)
    tool_calls: List[Dict] = field(default_factory=list)
    response_text: str = ""
    cost_usd: float = 0.0
    num_turns: int = 0
    error: Optional[str] = None


@dataclass
class EvalResult:
    eval_id: int
    name: str
    prompt: str
    passed: bool
    score: float
    checks: Dict[str, bool]
    notes: str
    bash_commands: List[str]
    files_read: List[str]
    cost_usd: float
    num_turns: int
    duration_s: float
    error: Optional[str]


# ---------------------------------------------------------------------------
# Claude runner
# ---------------------------------------------------------------------------


def run_claude(prompt: str, timeout: int = 300) -> SessionOutput:
    """Run claude -p in non-interactive stream-json mode."""
    # Unset CLAUDECODE so nested invocation doesn't refuse to start
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    cmd = [
        "claude",
        "-p",
        prompt,
        "--output-format",
        "stream-json",
        "--verbose",
        "--dangerously-skip-permissions",
        "--max-budget-usd",
        "1.00",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        s = SessionOutput()
        s.error = f"Timed out after {timeout}s"
        return s
    except FileNotFoundError:
        s = SessionOutput()
        s.error = "claude CLI not found — install Claude Code and ensure it is on PATH"
        return s

    return _parse_stream_json(proc.stdout, proc.stderr)


def _parse_stream_json(stdout: str, stderr: str) -> SessionOutput:
    """Parse newline-delimited JSON events from claude -p --output-format stream-json."""
    out = SessionOutput()

    for raw in stdout.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            continue

        etype = event.get("type", "")

        if etype == "assistant":
            for block in event.get("message", {}).get("content", []):
                btype = block.get("type", "")
                if btype == "text":
                    out.response_text += block.get("text", "")
                elif btype == "tool_use":
                    name = block.get("name", "")
                    inp = block.get("input", {})
                    out.tool_calls.append({"name": name, "input": inp})
                    if name == "Bash":
                        cmd_str = inp.get("command", "")
                        if cmd_str:
                            out.bash_commands.append(cmd_str)
                    elif name == "Read":
                        fpath = inp.get("file_path", "")
                        if fpath:
                            out.files_read.append(fpath)

        elif etype == "result":
            out.cost_usd = event.get("total_cost_usd", event.get("cost_usd", 0.0))
            out.num_turns = event.get("num_turns", 0)
            result_text = event.get("result", "")
            if result_text:
                out.response_text = result_text
            if event.get("subtype") == "error_max_turns":
                out.error = "Hit max-turns budget"

    if not out.response_text and not out.tool_calls and stderr.strip():
        out.error = stderr.strip()[:400]

    return out


# ---------------------------------------------------------------------------
# Grader helpers
# ---------------------------------------------------------------------------


def _bash(s: SessionOutput, *patterns: str) -> bool:
    text = "\n".join(s.bash_commands)
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def _text(s: SessionOutput, *patterns: str) -> bool:
    return any(re.search(p, s.response_text, re.IGNORECASE) for p in patterns)


def _read(s: SessionOutput, *substrings: str) -> bool:
    return any(sub in f for f in s.files_read for sub in substrings)


# ---------------------------------------------------------------------------
# Eval definitions
# ---------------------------------------------------------------------------

# Shared prompt for evals 1 and 5 (same task, different grading lens)
_ENV_LIFECYCLE_PROMPT = (
    "Enable DEV_TOOLING_ENABLED for the DataHub containers and confirm "
    "it is active in the running server."
)


@dataclass
class Eval:
    id: int
    name: str
    prompt: str
    # returns (passed, score 0-1, checks dict, notes)
    grade: Callable[[SessionOutput], Tuple[bool, float, Dict[str, bool], str]]
    # if set, reuse the session from this eval ID instead of running a new one
    reuse_session_from: Optional[int] = None


def _grade_env_lifecycle(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "ran `env set DEV_TOOLING_ENABLED`": _bash(
            s, r"env\s+set\s+DEV_TOOLING_ENABLED"
        ),
        "ran `env restart`": _bash(s, r"env\s+restart"),
        "verified via `env list` or saw pending_restart:false": (
            _bash(s, r"env\s+list") or _text(s, r"pending_restart")
        ),
        "did NOT stop after env set without restarting": not (
            _bash(s, r"env\s+set") and not _bash(s, r"env\s+restart")
        ),
    }
    score = sum(checks.values()) / len(checks)
    notes = _fmt(checks)
    return score >= 0.75, score, checks, notes


def _grade_module_mapping(s: SessionOutput) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "identified datahub-gms as target": (
            _text(s, r"datahub.?gms") or _bash(s, r"datahub.?gms")
        ),
        "used rebuild (not reset/nuke)": (
            _bash(s, r"\srebuild\b") and not _bash(s, r"\b(reset|nuke)\b")
        ),
        "did not unnecessarily rebuild all containers": not _bash(
            s, r"nuke|docker\s+rm"
        ),
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
        "explained data-safety reasoning": _text(
            s, r"keep.data|data.loss|volumes|escalat"
        ),
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
    # PASS = agent proactively read the docs  → hypothesis falsified (it CAN figure it out)
    # FAIL = agent flew blind without reading → hypothesis confirmed (it cannot)
    checks: Dict[str, bool] = {
        "read AGENTS.md": _read(s, "AGENTS.md"),
        "read any file in scripts/dev/": _read(s, "scripts/dev"),
    }
    any_read = any(checks.values())
    notes = _fmt(checks)
    if s.files_read:
        notes += f"\n  files_read: {s.files_read}"
    else:
        notes += "\n  files_read: (none)"
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


def _fmt(checks: Dict[str, bool]) -> str:
    return "  " + "\n  ".join(f"{'✓' if v else '✗'} {k}" for k, v in checks.items())


def _grade_search_agent_context(
    s: SessionOutput,
) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "discovered agent context (--agent-context, --help, or read file)": (
            _bash(
                s, r"datahub\s+search\s+--agent-context", r"datahub\s+search\s+--help"
            )
            or _read(s, "AGENT_CONTEXT.md")
        ),
        "used --dry-run before executing": _bash(s, r"--dry-run"),
        "used --filter for platform": _bash(s, r"--filter\s+platform=snowflake"),
        "used --urns-only or --format urns": _bash(s, r"--urns-only|--format\s+urns"),
        "set --limit explicitly": _bash(s, r"--limit\s+\d+"),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 0.8, score, checks, _fmt(checks)


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


def _grade_frontend_start(
    s: SessionOutput,
) -> Tuple[bool, float, Dict[str, bool], str]:
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


def _grade_setup_frontend(
    s: SessionOutput,
) -> Tuple[bool, float, Dict[str, bool], str]:
    checks: Dict[str, bool] = {
        "used `datahub-dev.sh setup frontend`": _bash(
            s, r"datahub.dev\S*\s+setup\s+frontend"
        ),
        "did NOT use raw `yarn install`": not _bash(s, r"yarn\s+install"),
    }
    score = sum(checks.values()) / len(checks)
    return score >= 1.0, score, checks, _fmt(checks)


def _grade_env_set_generic(
    s: SessionOutput,
) -> Tuple[bool, float, Dict[str, bool], str]:
    """Grade: did the agent use datahub-dev env set, or go ad-hoc?"""
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


DEFINED_EVALS: List[Eval] = [
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
        id=10,
        name="env set generic: use datahub-dev env set, not manual .env editing",
        prompt=(
            "Set ELASTICSEARCH_USE_SSL=true for the DataHub containers "
            "so they connect to Elasticsearch over TLS."
        ),
        grade=_grade_env_set_generic,
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
        id=11,
        name="search CLI: discover --agent-context, then use best practices",
        prompt=(
            "Use the DataHub CLI to find all Snowflake datasets in the PROD "
            "environment. I only need the URNs. "
            "Start by reading the search command's agent context, then verify "
            "your query with a dry run before executing."
        ),
        grade=_grade_search_agent_context,
    ),
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_evals(
    eval_ids: List[int],
    runs: int = 1,
    verbose: bool = False,
    dry_run: bool = False,
) -> List[List[EvalResult]]:
    """Run evals, returning a list of runs where each run is a list of EvalResults."""
    evals_to_run = [e for e in DEFINED_EVALS if e.id in eval_ids]
    all_runs: List[List[EvalResult]] = []

    for run_num in range(runs):
        if runs > 1:
            print(f"\n{'=' * 60}")
            print(f"RUN {run_num + 1} / {runs}")
            print(f"{'=' * 60}")

        run_results: List[EvalResult] = []
        # Cache sessions by prompt so evals with reuse_session_from share them
        session_cache: Dict[int, SessionOutput] = {}

        for ev in evals_to_run:
            print(f"\nEval {ev.id}: {ev.name}")
            print(f"  Prompt: {ev.prompt[:80]}{'...' if len(ev.prompt) > 80 else ''}")

            if dry_run:
                print("  [dry-run — skipping API call]")
                continue

            # Get or reuse session
            if (
                ev.reuse_session_from is not None
                and ev.reuse_session_from in session_cache
            ):
                session = session_cache[ev.reuse_session_from]
                print(f"  Reusing session from eval {ev.reuse_session_from}")
                duration = 0.0
            else:
                print("  Running claude...", end=" ", flush=True)
                t0 = time.time()
                session = run_claude(ev.prompt)
                duration = time.time() - t0
                session_cache[ev.id] = session

                if session.error:
                    print(f"ERROR: {session.error}")
                else:
                    print(
                        f"done ({duration:.0f}s, "
                        f"{session.num_turns} turns, "
                        f"${session.cost_usd:.4f})"
                    )

            if session.error:
                result = EvalResult(
                    eval_id=ev.id,
                    name=ev.name,
                    prompt=ev.prompt,
                    passed=False,
                    score=0.0,
                    checks={},
                    notes=f"Session error: {session.error}",
                    bash_commands=session.bash_commands,
                    files_read=session.files_read,
                    cost_usd=session.cost_usd,
                    num_turns=session.num_turns,
                    duration_s=duration,
                    error=session.error,
                )
            else:
                passed, score, checks, notes = ev.grade(session)
                result = EvalResult(
                    eval_id=ev.id,
                    name=ev.name,
                    prompt=ev.prompt,
                    passed=passed,
                    score=score,
                    checks=checks,
                    notes=notes,
                    bash_commands=session.bash_commands,
                    files_read=session.files_read,
                    cost_usd=session.cost_usd,
                    num_turns=session.num_turns,
                    duration_s=duration,
                    error=None,
                )

            status = "PASS" if result.passed else "FAIL"
            print(f"  Result: {status} (score={result.score:.0%})")
            print(result.notes)

            if verbose:
                print(f"  Bash commands ({len(result.bash_commands)}):")
                for cmd in result.bash_commands:
                    print(f"    $ {cmd[:120]}")
                print(f"  Files read: {result.files_read or '(none)'}")

            run_results.append(result)

        all_runs.append(run_results)

    return all_runs


# ---------------------------------------------------------------------------
# Summary + output
# ---------------------------------------------------------------------------


def print_summary(all_runs: List[List[EvalResult]]) -> None:
    if not all_runs or not all_runs[0]:
        return

    # Aggregate across runs
    eval_ids = [r.eval_id for r in all_runs[0]]
    eval_names = {r.eval_id: r.name for r in all_runs[0]}

    print(f"\n{'=' * 72}")
    print("SUMMARY")
    print(f"{'=' * 72}")
    print(f"{'ID':<4} {'Eval':<48} {'Pass':<8} {'Avg Score':<10}")
    print(f"{'-' * 4} {'-' * 48} {'-' * 8} {'-' * 10}")

    total_pass = 0
    total = 0

    for eid in eval_ids:
        results_for_eval = [r for run in all_runs for r in run if r.eval_id == eid]
        passes = sum(1 for r in results_for_eval if r.passed)
        avg_score = sum(r.score for r in results_for_eval) / len(results_for_eval)
        pass_str = f"{passes}/{len(results_for_eval)}"
        name = eval_names[eid][:48]
        print(f"{eid:<4} {name:<48} {pass_str:<8} {avg_score:.0%}")
        total_pass += passes
        total += len(results_for_eval)

    print(f"{'-' * 72}")
    total_cost = sum(r.cost_usd for run in all_runs for r in run)
    print(f"Overall: {total_pass}/{total} passed   Total cost: ${total_cost:.4f}")
    print(f"{'=' * 72}")

    # Interpretation for eval 5
    meta_results = [r for run in all_runs for r in run if r.eval_id == 5]
    if meta_results:
        meta_pass_rate = sum(1 for r in meta_results if r.passed) / len(meta_results)
        print()
        if meta_pass_rate >= 0.8:
            print(
                "Eval 5 (meta): Agent read the docs in "
                f"{meta_pass_rate:.0%} of runs → hypothesis FALSIFIED "
                "(agent figures it out on its own)"
            )
        elif meta_pass_rate <= 0.2:
            print(
                "Eval 5 (meta): Agent read the docs in "
                f"{meta_pass_rate:.0%} of runs → hypothesis CONFIRMED "
                "(@AGENTS.md import is needed)"
            )
        else:
            print(
                "Eval 5 (meta): Agent read the docs in "
                f"{meta_pass_rate:.0%} of runs → inconclusive, run more times"
            )


def results_to_json(all_runs: List[List[EvalResult]]) -> str:
    serializable = []
    for run in all_runs:
        serializable.append([asdict(r) for r in run])
    return json.dumps(serializable, indent=2)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Agent eval suite for DataHub dev tooling knowledge"
    )
    parser.add_argument(
        "--eval",
        type=str,
        default=None,
        help="Comma-separated eval IDs to run (default: all). E.g. --eval 1,3,5",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of times to run each eval (default: 1)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON (suppresses normal output)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show bash commands and files read for each eval",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print prompts without making API calls",
    )
    args = parser.parse_args()

    if args.eval:
        try:
            eval_ids = [int(x.strip()) for x in args.eval.split(",")]
        except ValueError:
            print(f"ERROR: --eval must be comma-separated integers, got: {args.eval}")
            return 1
        valid = {e.id for e in DEFINED_EVALS}
        invalid = set(eval_ids) - valid
        if invalid:
            print(f"ERROR: Unknown eval IDs: {invalid}. Valid: {sorted(valid)}")
            return 1
    else:
        eval_ids = [e.id for e in DEFINED_EVALS]

    if not args.json:
        print(f"Running {len(eval_ids)} eval(s), {args.runs} run(s) each")
        print(f"Repo: {REPO_ROOT}")
        print(
            "Note: evals run against the LIVE STACK. "
            "Eval 3 uses an advisory prompt (explain, don't nuke)."
        )

    all_runs = run_evals(
        eval_ids,
        runs=args.runs,
        verbose=args.verbose or args.json,
        dry_run=args.dry_run,
    )

    if args.json:
        print(results_to_json(all_runs))
    elif not args.dry_run:
        print_summary(all_runs)

    # Exit 1 if any eval failed
    any_failed = any(not r.passed for run in all_runs for r in run)
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
