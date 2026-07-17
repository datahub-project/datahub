"""
Shared infrastructure for DataHub agent eval suites.

Each pillar has its own test_evals_<pillar>.py that imports from here and
defines its own DEFINED_EVALS list. This module provides everything needed
to run, grade, and report evals.
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


@dataclass
class Eval:
    id: int
    name: str
    prompt: str
    # returns (passed, score 0-1, checks dict, notes)
    grade: Callable[[SessionOutput], Tuple[bool, float, Dict[str, bool], str]]
    # if set, reuse the session from this eval ID instead of running a new one
    reuse_session_from: Optional[int] = None


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


def _fmt(checks: Dict[str, bool]) -> str:
    return "  " + "\n  ".join(f"{'✓' if v else '✗'} {k}" for k, v in checks.items())


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_evals(
    evals: List[Eval],
    eval_ids: List[int],
    runs: int = 1,
    verbose: bool = False,
    dry_run: bool = False,
) -> List[List[EvalResult]]:
    """Run evals, returning a list of runs where each run is a list of EvalResults."""
    evals_to_run = [e for e in evals if e.id in eval_ids]
    all_runs: List[List[EvalResult]] = []

    for run_num in range(runs):
        if runs > 1:
            print(f"\n{'=' * 60}")
            print(f"RUN {run_num + 1} / {runs}")
            print(f"{'=' * 60}")

        run_results: List[EvalResult] = []
        session_cache: Dict[int, SessionOutput] = {}

        for ev in evals_to_run:
            print(f"\nEval {ev.id}: {ev.name}")
            print(f"  Prompt: {ev.prompt[:80]}{'...' if len(ev.prompt) > 80 else ''}")

            if dry_run:
                print("  [dry-run — skipping API call]")
                continue

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


def print_summary(
    all_runs: List[List[EvalResult]], meta_eval_id: Optional[int] = None
) -> None:
    if not all_runs or not all_runs[0]:
        return

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

    if meta_eval_id is not None:
        meta_results = [r for run in all_runs for r in run if r.eval_id == meta_eval_id]
        if meta_results:
            meta_pass_rate = sum(1 for r in meta_results if r.passed) / len(
                meta_results
            )
            print()
            if meta_pass_rate >= 0.8:
                print(
                    f"Eval {meta_eval_id} (meta): Agent read the docs in "
                    f"{meta_pass_rate:.0%} of runs → hypothesis FALSIFIED "
                    "(agent figures it out on its own)"
                )
            elif meta_pass_rate <= 0.2:
                print(
                    f"Eval {meta_eval_id} (meta): Agent read the docs in "
                    f"{meta_pass_rate:.0%} of runs → hypothesis CONFIRMED "
                    "(@AGENTS.md import is needed)"
                )
            else:
                print(
                    f"Eval {meta_eval_id} (meta): Agent read the docs in "
                    f"{meta_pass_rate:.0%} of runs → inconclusive, run more times"
                )


def results_to_json(all_runs: List[List[EvalResult]]) -> str:
    serializable = []
    for run in all_runs:
        serializable.append([asdict(r) for r in run])
    return json.dumps(serializable, indent=2)


# ---------------------------------------------------------------------------
# Reusable CLI main factory
# ---------------------------------------------------------------------------


def make_main(
    evals: List[Eval],
    description: str,
    meta_eval_id: Optional[int] = None,
) -> Callable[[], int]:
    """Return a main() function for a pillar eval file."""

    def main() -> int:
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument(
            "--eval",
            type=str,
            default=None,
            help="Comma-separated eval IDs to run (default: all). E.g. --eval 1,3",
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
                print(
                    f"ERROR: --eval must be comma-separated integers, got: {args.eval}"
                )
                return 1
            valid = {e.id for e in evals}
            invalid = set(eval_ids) - valid
            if invalid:
                print(f"ERROR: Unknown eval IDs: {invalid}. Valid: {sorted(valid)}")
                return 1
        else:
            eval_ids = [e.id for e in evals]

        if not args.json:
            print(f"Running {len(eval_ids)} eval(s), {args.runs} run(s) each")
            print(f"Repo: {REPO_ROOT}")

        all_runs = run_evals(
            evals,
            eval_ids,
            runs=args.runs,
            verbose=args.verbose or args.json,
            dry_run=args.dry_run,
        )

        if args.json:
            print(results_to_json(all_runs))
        elif not args.dry_run:
            print_summary(all_runs, meta_eval_id=meta_eval_id)

        any_failed = any(not r.passed for run in all_runs for r in run)
        return 1 if any_failed else 0

    return main
