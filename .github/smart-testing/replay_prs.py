"""Replay past PRs through the classifier and check what they would run now.

``golden_prs.yaml`` lists merged PRs with their changed files, what smart
testing did at the time (``then``, copied from the PR's smart-testing comment)
and what the pytest selection should be now (``expect``). The SaaS fork keeps
its own PRs in ``golden_prs.saas.yaml``: when that file exists it is replayed
instead, and ``golden_prs.yaml``, synced from OSS, is ignored. ``run``
classifies each PR with the current ``classify_pr.py`` and ``taxonomy.yaml``,
names the selection with ``selection.py``, and reports one outcome per run:

    match          the selection and the expected tags are as expected
    coverage-risk  narrower than expected: canary or plumbing where smart or p0
                   was expected, or none of the expected feature tags
    costlier       broader than expected, e.g. p0 where canary or smart was
                   expected: safe, but the batches were not saved. A p0
                   fail-safe has no tags, so expected feature tags are not
                   checked for it
    tags-differ    the expected selection, but an expected change-type tag is
                   missing
    needs-model    the paths alone do not classify the PR, and --offline was given
    error          the classifier raised

PRs whose files all match change-type ``paths`` are classified without the
model, so ``run --offline`` needs no credentials (CI runs it as a unit test).
The rest call the model with the same request CI sends, patches included. The
Smart Testing Replay workflow runs them on every PR that changes
``.github/smart-testing/``, using Workload Identity Federation (``ci_replay.sh``).

Usage:
    python .github/smart-testing/replay_prs.py run --offline       # no model calls
    python .github/smart-testing/replay_prs.py run --runs 3        # needs model credentials
    python .github/smart-testing/replay_prs.py fetch 14167 14200   # print new entries

``run`` and ``fetch`` read PRs with ``gh api``. ``run`` exits 1 when most runs
of a PR are coverage-risk or error (2 of 3 with ``--runs 3``). The classifier
does not always answer the same way, so a PR that fails fewer of its runs is
listed as a warning instead.
"""

import argparse
import contextlib
import io
import json
import os
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Set

import yaml

import classify_pr
import selection
from smart_testing_constants import CHANGE_TYPE, FEATURE_DIMENSIONS

GOLDEN_YAML = Path(__file__).resolve().parent / "golden_prs.yaml"
# The SaaS fork's own golden PRs; replayed instead of golden_prs.yaml when present.
GOLDEN_SAAS_YAML = GOLDEN_YAML.with_name("golden_prs.saas.yaml")
COMMENT_MARKER = "<!-- smart-testing-selection -->"
# How much each selection runs, narrowest first.
BREADTH = {"canary": 0, "plumbing": 1, "smart": 2, "p0": 3}
FAILING = ("coverage-risk", "error")


@dataclass(frozen=True)
class Expectation:
    selections: Sequence[str]
    features_any: Set[str] = field(default_factory=set)
    change_type: Set[str] = field(default_factory=set)


@dataclass(frozen=True)
class GoldenPR:
    number: int
    base: str
    head: str
    files: Sequence[str]
    expect: Expectation
    why: str


@dataclass(frozen=True)
class Replay:
    selection: str
    tags: Dict[str, List[str]]
    failsafe: str = ""


def golden_file() -> Path:
    return GOLDEN_SAAS_YAML if GOLDEN_SAAS_YAML.exists() else GOLDEN_YAML


def load_golden() -> List[GoldenPR]:
    data = yaml.safe_load(golden_file().read_text())
    prs = []
    for entry in data["prs"]:
        expect = entry["expect"]
        selections = expect["selection"]
        prs.append(
            GoldenPR(
                number=entry["number"],
                base=entry["base"],
                head=entry["head"],
                files=entry["files"],
                expect=Expectation(
                    selections=[selections]
                    if isinstance(selections, str)
                    else selections,
                    features_any=set(expect.get("features-any") or []),
                    change_type=set(expect.get(CHANGE_TYPE) or []),
                ),
                why=entry["why"],
            )
        )
    return prs


def feature_tags(tags: Dict[str, List[str]]) -> Set[str]:
    return {tag for dim in FEATURE_DIMENSIONS for tag in tags.get(dim, [])}


def judge(expect: Expectation, got: Replay) -> str:
    if got.selection not in BREADTH:
        return "error"
    if got.selection == "p0":
        # The fail-safe has no tags to check: it runs the widest tier instead.
        return "match" if "p0" in expect.selections else "costlier"
    if expect.features_any and not expect.features_any & feature_tags(got.tags):
        return "coverage-risk"
    if got.selection not in expect.selections:
        widest = max(BREADTH[s] for s in expect.selections)
        return "costlier" if BREADTH[got.selection] > widest else "coverage-risk"
    if expect.change_type - set(got.tags.get(CHANGE_TYPE, [])):
        return "tags-differ"
    return "match"


def failure_reason(expect: Expectation, got: Replay) -> str:
    """Why judge() called this run coverage-risk or error, in one line."""
    if got.selection not in BREADTH:
        return f"unknown selection {got.selection!r}"
    features = feature_tags(got.tags)
    if expect.features_any and not expect.features_any & features:
        return (
            f"none of the expected feature tags ({', '.join(sorted(expect.features_any))}); "
            f"got {', '.join(sorted(features)) or 'no feature tags'}"
        )
    return f"selection {got.selection} is narrower than {'|'.join(expect.selections)}"


def is_path_only(files: Sequence[str]) -> bool:
    rules = classify_pr.load_path_rules()
    return all(classify_pr.change_types_for(path, rules) for path in files)


def fetch_changes(repo: str, pr: GoldenPR) -> List[classify_pr.ChangedFile]:
    """The PR's files with patches, from the same compare call CI makes."""
    os.environ.update(
        {"GITHUB_REPOSITORY": repo, "BASE_SHA": pr.base, "HEAD_SHA": pr.head}
    )
    return classify_pr.get_pr_changes()


def replay(repo: str, pr: GoldenPR, verbose: bool) -> Replay:
    if is_path_only(pr.files):
        changed = [
            classify_pr.ChangedFile(path=p, status="modified", patch="")
            for p in pr.files
        ]
    else:
        changed = fetch_changes(repo, pr)
    log = io.StringIO()
    with contextlib.redirect_stderr(sys.stderr if verbose else log):
        try:
            smart_tags = classify_pr.classify_changes(changed, dry_run=False).to_json()
        except classify_pr.FailSafe as exc:
            return Replay(selection="p0", tags={}, failsafe=exc.reason.value)
    tags = json.loads(smart_tags)
    tags.pop("confidence", None)
    return Replay(selection=selection.selection_for(smart_tags), tags=tags)


def describe(got: Replay) -> str:
    if got.failsafe:
        return f"p0 ({got.failsafe})"
    tags = "; ".join(
        f"{dim}: {','.join(values)}" for dim, values in sorted(got.tags.items())
    )
    return f"{got.selection} [{tags or 'no tags'}]"


def fails_most_runs(failed: int, runs: int) -> bool:
    """A PR fails the replay when most of its runs fail: 2 of 3 in CI."""
    return failed * 2 > runs


def print_section(title: str, reports: List[str], annotation: str) -> None:
    """Print a FAILED or WARNING section of the report, if it has any PRs."""
    if not reports:
        return
    print(f"\n{title}\n")
    print("\n\n".join(reports))
    if os.environ.get("GITHUB_ACTIONS") == "true":
        for report in reports:
            # Workflow command: shows each PR as an annotation on the run.
            first, _, body = report.partition("\n")
            print(
                f"::{annotation} title=Replay {first}::{body.replace(chr(10), '%0A')}"
            )


def cmd_run(
    repo: str, prs: List[GoldenPR], offline: bool, runs: int, verbose: bool
) -> int:
    outcomes: Counter = Counter()
    failures: List[str] = []
    warnings: List[str] = []
    for pr in prs:
        expected = "|".join(pr.expect.selections)
        if offline and not is_path_only(pr.files):
            outcomes["needs-model"] += 1
            print(f"#{pr.number:<6} needs-model    expected {expected}: {pr.why}")
            continue
        failed_runs: List[str] = []
        for attempt in range(runs):
            try:
                got = replay(repo, pr, verbose)
                outcome = judge(pr.expect, got)
                detail = describe(got)
                reason = failure_reason(pr.expect, got)
            except Exception as exc:  # report it and keep replaying the rest
                outcome, detail = "error", repr(exc)
                reason = f"classifier raised {detail}"
            outcomes[outcome] += 1
            run_label = f" run {attempt + 1}" if runs > 1 else ""
            print(
                f"#{pr.number:<6} {outcome:<14} expected {expected}, got {detail}{run_label}"
            )
            if outcome in FAILING:
                failed_runs.append(f"run {attempt + 1}: {outcome}: {reason}")
        if not failed_runs:
            continue
        report = (
            f"#{pr.number} failed {len(failed_runs)} of {runs} runs "
            f"(expected {expected})\n  why: {pr.why}\n"
            + "\n".join(f"  {line}" for line in failed_runs)
        )
        if fails_most_runs(len(failed_runs), runs):
            failures.append(report)
        else:
            warnings.append(report)
    print(
        "\n" + ", ".join(f"{name}: {count}" for name, count in sorted(outcomes.items()))
    )
    print_section(
        f"FAILED: {len(failures)} PR(s) with coverage-risk or error in most runs",
        failures,
        "error",
    )
    print_section(
        f"WARNING: {len(warnings)} PR(s) with coverage-risk or error in some runs,"
        " but not most",
        warnings,
        "warning",
    )
    return 1 if failures else 0


def gh_json(path: str, paginate: bool = False) -> object:
    args = ["gh", "api", path, *(["--paginate", "--slurp"] if paginate else [])]
    out = subprocess.run(
        args, capture_output=True, text=True, timeout=120, check=True
    ).stdout
    data = json.loads(out)
    return [item for page in data for item in page] if paginate else data


def comment_fields(body: str) -> Dict[str, str]:
    """What the PR's smart-testing comment said about the pytest selection."""
    section = body.split("### pytest smoke tests", 1)[-1].split("\n### ", 1)[0]
    patterns = {
        "selection": r"\*\*Selection:\*\* `([^`]+)`",
        "applied": r"applied: `([^`]+)`",
        "size": r"\*\*Size:\*\* (.+)",
        "tags": r"\*\*Tags:\*\* (.+)",
        "confidence": r"\*\*Confidence:\*\* (\w+)",
        "failsafe": r"\*\*Fell back to the fail-safe because:\*\* (.+)",
    }
    found = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, section)
        if match:
            found[key] = match.group(1).replace("`", "").strip()
    return found


def fetch_entries(repo: str, numbers: Sequence[int]) -> Iterator[Dict[str, object]]:
    for number in numbers:
        pr = gh_json(f"repos/{repo}/pulls/{number}")
        assert isinstance(pr, dict)
        base, head = pr["base"]["sha"], pr["head"]["sha"]
        compare = gh_json(f"repos/{repo}/compare/{base}...{head}")
        assert isinstance(compare, dict)
        comments = gh_json(f"repos/{repo}/issues/{number}/comments", paginate=True)
        assert isinstance(comments, list)
        bodies = [
            c["body"] for c in comments if COMMENT_MARKER in (c.get("body") or "")
        ]
        yield {
            "number": number,
            "base": base,
            "head": head,
            "files": sorted(f["filename"] for f in compare.get("files") or []),
            "then": comment_fields(bodies[-1]) if bodies else {},
            "expect": {"selection": "TODO"},
            "why": "TODO",
        }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--repo", help="owner/repo; default: the golden file's repository"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="replay the golden PRs")
    run.add_argument(
        "--offline", action="store_true", help="skip PRs that need the model"
    )
    run.add_argument("--runs", type=int, default=1, help="classifications per PR")
    run.add_argument("--pr", type=int, action="append", help="only these PRs")
    run.add_argument(
        "-v", "--verbose", action="store_true", help="show classifier logs"
    )
    fetch = sub.add_parser(
        "fetch", help="print golden entries for PRs, to fill in and add"
    )
    fetch.add_argument("numbers", type=int, nargs="+")
    args = parser.parse_args(argv)

    repo = args.repo or yaml.safe_load(golden_file().read_text())["repository"]
    if args.command == "fetch":
        print(yaml.safe_dump(list(fetch_entries(repo, args.numbers)), sort_keys=False))
        return 0
    prs = [pr for pr in load_golden() if not args.pr or pr.number in args.pr]
    return cmd_run(repo, prs, args.offline, args.runs, args.verbose)


if __name__ == "__main__":
    sys.exit(main())
