#!/usr/bin/env python3
"""Plan the Playwright PR P0 tier: which tests a pull request needs.

The tier runs, instead of the whole suite:

* every test tagged ``@p0``, and
* every test Playwright says the change affects: ``--only-changed`` follows
  Playwright's own dependency graph (imports, path aliases, ``require``, re-exports),
  so a spec runs in full when it, or anything it loads, changed, and
* every spec that seeds from a changed ``tests/<feature>/fixtures/data.json``. Seed
  data is read with ``fs`` by the seeding fixture, so the graph can't see it. A spec
  seeds from its own directory's data (``featureName`` is its directory), or from
  another directory's when listed in SEED_BORROWERS; check_playwright_p0.py fails
  on any spec that does neither, so this mapping can't drift.

The full suite runs instead when a changed file matches FULL_SUITE_PATTERNS: the
Playwright config, package files, the tier's own scripts and reusable workflows,
global seed data (``test-data/``, loaded for every suite) and any other JSON under
``tests/``. ``docker-unified.yml`` is deliberately not a trigger: it changes often
for unrelated jobs, and a broken edit to the tier's wiring there fails the plan job,
which also runs the full suite.

Inputs are Playwright's own listings, so there is no TypeScript parsing here:

    npx playwright test --list --reporter=json                       > full.json
    npx playwright test --list --reporter=json --only-changed=<base> > changed.json

Either listing failing, including a spec that no longer loads (say it imports a
deleted helper), also runs the full suite. Files Playwright never loads, such as
types-only modules, docs or scripts, select no tests.

``--only-changed`` also counts uncommitted and untracked files, so run this on a
clean checkout of the PR (as CI does); on a dirty tree it selects extra tests.

Output: a plan JSON (the ``playwright-tier-plan`` artifact) that
split_playwright_tests.py --tier-plan packs into per-shard ``--test-list`` files,
and GitHub outputs (``run_mode``, ``total_shards``).

Shards: the tier runs on as many shards as the full suite, so P0 is spread out for
the shortest wall time, never more shards than it has spec files, since a file
is never split.

Weights: the weekly refresh (update-test-weights.yml) only reads successful runs on
the default branch, which never run a tier. Keep it that way: a tier run's per-file durations
cover only the selected tests and would pull the weights down.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from split_playwright_tests import load_weights, scaled_spec_weights

PLAYWRIGHT_DIR = "e2e-test/ui/playwright"
TESTS_DIR = f"{PLAYWRIGHT_DIR}/tests"

# Changes Playwright's dependency graph can't map to particular tests.
FULL_SUITE_PATTERNS = (
    re.compile(rf"^{PLAYWRIGHT_DIR}/playwright(\.[\w-]+)?\.config\.ts$"),
    re.compile(rf"^{PLAYWRIGHT_DIR}/(package\.json|yarn\.lock|tsconfig\.json)$"),
    # Read with fs, so the graph can't see them: seeding.fixture.ts loads
    # test-data/data.json for every suite. A feature's fixtures/data.json is mapped
    # by SEED_DATA first; any other JSON under tests/ has no known reader.
    re.compile(rf"^{PLAYWRIGHT_DIR}/test-data/"),
    re.compile(rf"^{TESTS_DIR}/.+\.json$"),
    re.compile(
        r"^\.github/(scripts/(plan_playwright_tier|split_playwright_tests)\.py"
        r"|workflows/(resusable-playwright-tests|reusable-playwright-report)\.yml)$"
    ),
)
SPEC_FILE = re.compile(rf"^{TESTS_DIR}/.+\.spec\.ts$")
# seeding.fixture.ts dataFilePath(): tests/<featureName>/fixtures/data.json.
SEED_DATA = re.compile(rf"^{TESTS_DIR}/([^/]+)/fixtures/data\.json$")
# featureName -> specs (relative to the tests dir) outside tests/<featureName>/ that
# seed from it. check_playwright_p0.py fails on any spec seeding from another
# directory without an entry here.
SEED_BORROWERS: dict[str, tuple[str, ...]] = {}

P0_TAG = "p0"  # Playwright's JSON reporter lists tags without their leading '@'
# Measured: the "Run Playwright tests" step takes ~1.4x what the weights predict.
WEIGHT_INFLATION = 1.4


@dataclass(frozen=True)
class ListedTest:
    file: str  # relative to the tests dir, e.g. "search/search.spec.ts"
    titles: tuple[str, ...]  # describe titles then the test title
    tags: tuple[str, ...]
    project: str

    @property
    def test_list_line(self) -> str:
        """This test as a ``--test-list`` line (the format ``--list`` prints, minus line:col)."""
        return " › ".join([f"[{self.project}]", self.file, *self.titles])


def listed_tests(playwright_list: dict[str, Any]) -> list[ListedTest]:
    tests = []

    def walk(suite: dict[str, Any], titles: tuple[str, ...]) -> None:
        for spec in suite.get("specs", []):
            for test in spec.get("tests", []):
                tests.append(
                    ListedTest(
                        file=spec["file"],
                        titles=(*titles, spec["title"]),
                        tags=tuple(spec.get("tags", [])),
                        project=test.get("projectName", ""),
                    )
                )
        for child in suite.get("suites", []):
            walk(child, (*titles, child["title"]))

    for file_suite in playwright_list.get("suites", []):
        walk(file_suite, ())
    return tests


@dataclass
class Selection:
    """The P0 selection for a change: what it would run, whether or not the tier is on.

    ``mode`` is "p0" (run the selected tests) or "full" (the change can't be narrowed).
    Whether this CI run actually uses the selection is decided separately, in
    plan_json(), from whether the tier was requested.
    """

    mode: str  # "p0" or "full"
    reason: str
    # spec path (relative to the tests dir) -> why it is selected
    reasons: dict[str, list[str]] = field(default_factory=dict)
    # specs that run only their P0 tests, as --test-list lines; every other selected spec runs in full
    p0_only: dict[str, list[str]] = field(default_factory=dict)
    # spec path -> (selected tests, all tests in the file)
    test_counts: dict[str, tuple[int, int]] = field(default_factory=dict)


def decide(
    tests: list[ListedTest], affected: list[ListedTest], changed: list[str]
) -> Selection:
    """The P0 selection for a change.

    ``affected`` is Playwright's ``--only-changed`` listing for the change, and
    ``changed`` the repo paths it touches.
    """
    reasons: dict[str, list[str]] = {}

    def add(spec: str, reason: str) -> None:
        if reason not in reasons.setdefault(spec, []):
            reasons[spec].append(reason)

    specs = {test.file for test in tests}
    for path in changed:
        if seed := SEED_DATA.match(path):
            feature = seed.group(1)
            seeded = {
                spec for spec in specs if spec.startswith(f"{feature}/")
            } | (set(SEED_BORROWERS.get(feature, ())) & specs)
            if not seeded:
                return Selection("full", f"{path} seeds no spec Playwright lists")
            for spec in seeded:
                add(spec, f"seeds from {feature}")
        elif any(p.match(path) for p in FULL_SUITE_PATTERNS):
            return Selection("full", f"{path} affects every test")

    for test in tests:
        if P0_TAG in test.tags:
            add(test.file, "p0")
    changed_specs = {
        path.removeprefix(f"{TESTS_DIR}/") for path in changed if SPEC_FILE.match(path)
    }
    for test in affected:
        add(
            test.file,
            "changed" if test.file in changed_specs else "affected by the change",
        )
    # An empty selection would give playwright_test an empty matrix, which skips the
    # job, so a PR could merge with no Playwright run at all.
    if not reasons:
        return Selection("full", "no @p0 test and no test affected by the change")

    totals: dict[str, int] = {}
    p0_lines: dict[str, list[str]] = {}
    for test in tests:
        totals[test.file] = totals.get(test.file, 0) + 1
        if P0_TAG in test.tags:
            p0_lines.setdefault(test.file, []).append(test.test_list_line)

    selection = Selection(
        "p0", "P0 tests plus the tests the change affects", reasons=reasons
    )
    for spec, why in sorted(reasons.items()):
        if why == ["p0"]:
            selection.p0_only[spec] = p0_lines[spec]
            selection.test_counts[spec] = (len(p0_lines[spec]), totals.get(spec, 0))
        else:
            selection.test_counts[spec] = (totals.get(spec, 0), totals.get(spec, 0))
    return selection


def shard_count(
    selection: Selection, weights: dict[str, float], suite_shards: int
) -> tuple[int, float]:
    if selection.mode != "p0" or not selection.reasons:
        return 0, 0.0
    predicted = (
        sum(scaled_spec_weights(selection.test_counts, weights).values())
        * WEIGHT_INFLATION
    )
    return max(1, min(suite_shards, len(selection.reasons))), predicted


def plan_json(
    selection: Selection, tier_requested: bool, shards: int, predicted: float
) -> dict[str, Any]:
    """The plan artifact: what this run does (``run*``) and what the selection decided (``selection*``)."""
    return {
        "runMode": selection.mode if tier_requested else "full",
        "runReason": selection.reason
        if tier_requested
        else "P0 tier not requested (shadow: selection shown only)",
        "tierRequested": tier_requested,
        "selectionMode": selection.mode,
        "selectionReason": selection.reason,
        "totalShards": shards,
        "predictedSeconds": round(predicted, 1),
        "specs": [
            {
                "path": spec,
                "reasons": why,
                "wholeFile": spec not in selection.p0_only,
                "testLines": selection.p0_only.get(spec, []),
                "selectedTests": selection.test_counts[spec][0],
                "totalTests": selection.test_counts[spec][1],
            }
            for spec, why in sorted(selection.reasons.items())
            if selection.mode == "p0"
        ],
    }


def summary_markdown(plan: dict[str, Any]) -> str:
    lines = [
        "## Playwright P0 tier",
        "",
        f"**This run:** `{plan['runMode']}` — {plan['runReason']}",
        "",
        f"**P0 selection:** `{plan['selectionMode']}` — {plan['selectionReason']}",
        "",
    ]
    if plan["selectionMode"] == "p0":
        selected = sum(spec["selectedTests"] for spec in plan["specs"])
        lines += [
            (
                f"Tier selection: {selected} test(s) in {len(plan['specs'])} spec file(s), "
                f"~{plan['predictedSeconds'] / 60:.1f} min predicted, {plan['totalShards']} shard(s)."
            ),
            "",
            "| Spec | Tests | Why |",
            "|---|---:|---|",
            *(
                f"| `{s['path']}` | {s['selectedTests']}/{s['totalTests']} | {', '.join(s['reasons'])} |"
                for s in plan["specs"]
            ),
        ]
    return "\n".join(lines) + "\n"


# ---- git and Playwright access (CI only; decide() above is pure) ----


def changed_files(repo_root: Path, base: str) -> list[str]:
    # --no-renames lists a rename as its deleted old path plus its added new path. Its
    # importers must change in the same PR, so --only-changed selects them, and a
    # missed one fails --list.
    return subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", base, "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()


def playwright_list(repo_root: Path, *args: str) -> dict[str, Any]:
    """Playwright's JSON listing. Raises when it fails or a spec doesn't load.

    A spec that fails to load (for example, it imports a deleted helper) is reported
    under ``errors`` with its tests missing, so it must not be read as "no tests".
    """
    command = " ".join(["--list", *args])
    # --no-install: never fetch a different Playwright when node_modules is missing.
    result = subprocess.run(
        ["npx", "--no-install", "playwright", "test", "--list", "--reporter=json", *args],
        cwd=repo_root / PLAYWRIGHT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        listing = json.loads(result.stdout)
    except ValueError as exc:
        raise RuntimeError(
            f"playwright {command} printed no JSON (exit {result.returncode}): "
            f"{result.stderr[-2000:]}"
        ) from exc
    if errors := listing.get("errors"):
        first = (errors[0].get("message") or "").splitlines()[0]
        raise RuntimeError(f"playwright {command}: {len(errors)} load error(s): {first}")
    if result.returncode != 0:
        raise RuntimeError(
            f"playwright {command} exited {result.returncode}: {result.stderr[-2000:]}"
        )
    return listing


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan the Playwright PR P0 tier.")
    parser.add_argument(
        "--base",
        required=True,
        help="Git ref the PR is compared with (the merge commit's HEAD^1)",
    )
    parser.add_argument("--tier-requested", choices=["true", "false"], default="false")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument(
        "--suite-shards",
        type=int,
        required=True,
        help="Shard count of the full-suite matrix; the tier uses the same",
    )
    parser.add_argument("--plan-out", type=Path, required=True)
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()
    tier_requested = args.tier_requested == "true"

    try:
        full = listed_tests(playwright_list(repo_root))
        affected = listed_tests(
            playwright_list(repo_root, f"--only-changed={args.base}")
        )
        print(
            f"plan_playwright_tier: {len(full)} tests listed, "
            f"{sum(P0_TAG in t.tags for t in full)} tagged @p0, "
            f"{len(affected)} affected by the change (--only-changed={args.base})",
            file=sys.stderr,
        )
        selection = decide(full, affected, changed_files(repo_root, args.base))
    except (RuntimeError, subprocess.CalledProcessError, OSError) as exc:
        reason = f"planning failed: {exc}".splitlines()[0]
        print(
            f"plan_playwright_tier: cannot plan, running the full suite: {exc}",
            file=sys.stderr,
        )
        # A workflow annotation, so a broken tier shows on the run instead of only in the log.
        print(f"::warning title=Playwright P0 tier::{reason}; running the full suite")
        selection = Selection("full", reason)

    weights = load_weights(
        str(repo_root / PLAYWRIGHT_DIR / "playwright_test_weights.json")
    )
    shards, predicted = shard_count(selection, weights, args.suite_shards)
    plan = plan_json(selection, tier_requested, shards, predicted)
    args.plan_out.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")

    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as fh:
            fh.write(
                f"run_mode={plan['runMode']}\ntotal_shards={plan['totalShards']}\n"
            )
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(summary_markdown(plan))
    print(
        json.dumps(
            {k: v for k, v in plan.items() if k != "specs"}
            | {"specFiles": len(plan["specs"])}
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
