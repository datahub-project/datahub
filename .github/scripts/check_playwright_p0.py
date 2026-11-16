#!/usr/bin/env python3
"""Fail on Playwright P0, domain and seed tagging the PR tier cannot rely on.

Three rules:

1. A P0 tag never sits on a single test inside a serial spec. The PR tier
   selects P0 tests with ``--grep @p0``. In a ``mode: 'serial'`` describe,
   later tests depend on state the earlier ones created (see
   tests/incidents-v2/v2-incidents.spec.ts: create, then update the same
   incident). Tagging one test of such a chain makes the tier run it without
   its predecessors, so it fails, or worse, passes against leftover state.
   Tagging the describe instead selects the whole chain.
2. Every test carries a ``DomainTag`` (``@domain:<name>``) naming a known
   domain. The domain fixture writes it into JUnit, which is how a failure
   reaches its owner.
3. Every ``featureName`` is a string literal (or an array of them) naming the
   spec's own directory, or is declared in SEED_BORROWERS in
   plan_playwright_tier.py. The tier maps a changed
   ``tests/<feature>/fixtures/data.json`` to the specs in ``tests/<feature>/`` plus
   those borrowers, so an undeclared borrower would be skipped when its seed data
   changes. Each SEED_BORROWERS entry must also still seed from its feature.

Rules 1 and 3 are static and stdlib-only, like check_p0_dependencies.py, so the
pre-commit hook runs them in seconds without Node. Rule 1 is deliberately
conservative: in a file that turns on serial mode anywhere, every P0 tag must
sit on a ``test.describe`` call.

Rule 2 runs when ``--playwright-list`` points at the output of
``npx playwright test --list --reporter=json``. Playwright's own listing sees
every test, including ones declared in loops, which a static scan cannot.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from plan_playwright_tier import SEED_BORROWERS

REPO_ROOT = Path(__file__).resolve().parents[2]
PLAYWRIGHT_ROOT = REPO_ROOT / "e2e-test" / "ui" / "playwright"
TESTS_ROOT = PLAYWRIGHT_ROOT / "tests"
TEST_TAGS = PLAYWRIGHT_ROOT / "utils" / "test-tags.ts"
DOMAIN_ENUM_RE = re.compile(r"export enum DomainTag \{(?P<body>[^}]*)\}")
DOMAIN_VALUE_RE = re.compile(r"=\s*'@domain:([a-z_]+)'")
# Playwright's JSON reporter lists tags without their leading '@'.
DOMAIN_TAG_PREFIX = "domain:"

SERIAL_RE = re.compile(r"mode:\s*['\"]serial['\"]|test\.describe\.serial\s*\(")
# What makes a test P0: the enum member or the raw tag.
P0_MARKER_RE = re.compile(r"TestTag\.P0\b|['\"]@p0['\"]")
# The calls a details argument can belong to. The longest alternatives come
# first so `test.describe.only(` is not read as `test(`.
CALL_RE = re.compile(
    r"\btest(?P<kind>\.describe(?:\.(?:serial|parallel|only|skip|fixme))?"
    r"|\.(?:only|skip|fixme|fail|slow))?\s*\("
)

SERIAL_MESSAGE = (
    "P0 tag on a single test in a serial spec; tag the enclosing test.describe instead"
)

# Strings first, so a "//" inside one isn't read as a comment.
STRING_OR_COMMENT_RE = re.compile(
    r"""'(?:\\.|[^'\\\n])*'|"(?:\\.|[^"\\\n])*"|`(?:\\.|[^`\\])*`|/\*.*?\*/|//[^\n]*""",
    re.S,
)
FEATURE_NAME_RE = re.compile(r"\bfeatureName\b")
FEATURE_VALUE_RE = re.compile(
    r"""featureName\s*:\s*(?P<value>\[[^\]]*\]|'[^'\n]*'|"[^"\n]*")"""
)
LITERAL_RE = re.compile(r"'([^'\n]*)'|\"([^\"\n]*)\"")


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.message}"


def _owning_call_is_describe(source: str, marker_start: int) -> bool | None:
    """Whether the nearest test call opened before the marker is a describe."""
    last = None
    for match in CALL_RE.finditer(source, 0, marker_start):
        last = match
    if last is None:
        return None
    return (last.group("kind") or "").startswith(".describe")


def check_source(path: Path, source: str) -> list[Violation]:
    if not SERIAL_RE.search(source):
        return []
    return [
        Violation(path, source.count("\n", 0, marker.start()) + 1, SERIAL_MESSAGE)
        for marker in P0_MARKER_RE.finditer(source)
        if _owning_call_is_describe(source, marker.start()) is False
    ]


def _without_comments(source: str) -> str:
    """The source with comments blanked out, keeping every offset and newline."""

    def blank(match: re.Match[str]) -> str:
        text = match.group(0)
        return re.sub(r"[^\n]", " ", text) if text.startswith("/") else text

    return STRING_OR_COMMENT_RE.sub(blank, source)


def seed_features(source: str) -> tuple[list[tuple[int, str]], list[int]]:
    """``featureName`` values in a spec: ([(line, name)], [lines that aren't literals])."""
    code = _without_comments(source)
    names, unreadable = [], []
    for match in FEATURE_NAME_RE.finditer(code):
        line = code.count("\n", 0, match.start()) + 1
        value = FEATURE_VALUE_RE.match(code, match.start())
        literals = LITERAL_RE.findall(value.group("value")) if value else []
        rest = LITERAL_RE.sub("", value.group("value")).strip("[] \n,") if value else ""
        if not value or rest:
            unreadable.append(line)
            continue
        names.extend((line, single or double) for single, double in literals)
    return names, unreadable


def check_seed_features(
    rel: str, source: str, borrowers: dict[str, tuple[str, ...]]
) -> list[tuple[int, str]]:
    """Rule 3 for one spec; ``rel`` is its path relative to the tests dir."""
    own = rel.split("/")[0]
    names, unreadable = seed_features(source)
    problems = [
        (
            line,
            "featureName must be a string literal or an array of them, so the PR tier "
            "can map seed data to this spec",
        )
        for line in unreadable
    ]
    problems.extend(
        (
            line,
            f"seeds from tests/{name}/ outside its own directory; add it to "
            f"SEED_BORROWERS['{name}'] in .github/scripts/plan_playwright_tier.py",
        )
        for line, name in names
        if name != own and rel not in borrowers.get(name, ())
    )
    return problems


def check_seed_borrowers(
    sources: dict[str, str], borrowers: dict[str, tuple[str, ...]]
) -> list[str]:
    """Every SEED_BORROWERS entry names a spec that still seeds from that feature."""
    return [
        f"SEED_BORROWERS['{feature}'] lists {rel}, which "
        + ("doesn't exist" if rel not in sources else f"no longer seeds from {feature}")
        for feature, specs in sorted(borrowers.items())
        for rel in specs
        if rel not in sources
        or feature not in {name for _, name in seed_features(sources[rel])[0]}
    ]


def known_domains(test_tags_source: str) -> set[str]:
    """The domains named by the ``DomainTag`` enum in utils/test-tags.ts."""
    match = DOMAIN_ENUM_RE.search(test_tags_source)
    return set(DOMAIN_VALUE_RE.findall(match.group("body"))) if match else set()


def _listed_tests(
    suite: dict[str, Any], titles: list[str]
) -> list[tuple[str, list[str]]]:
    """("file:line title", tags) for every test under a JSON-reporter suite."""
    tests = []
    for spec in suite.get("specs", []):
        title = " › ".join([*titles, spec["title"]])
        tests.append(
            (f"{spec.get('file')}:{spec.get('line')} {title}", spec.get("tags", []))
        )
    for child in suite.get("suites", []):
        tests.extend(_listed_tests(child, [*titles, child["title"]]))
    return tests


def check_domain_tags(playwright_list: dict[str, Any], domains: set[str]) -> list[str]:
    problems = []
    for file_suite in playwright_list.get("suites", []):
        for where, tags in _listed_tests(file_suite, []):
            owners = {
                tag[len(DOMAIN_TAG_PREFIX) :]
                for tag in tags
                if tag.startswith(DOMAIN_TAG_PREFIX)
            }
            if not owners:
                problems.append(f"{where}: no DomainTag; add one to its tag array")
            for unknown in sorted(owners - domains):
                problems.append(f"{where}: unknown domain tag @domain:{unknown}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check Playwright P0 and domain tags.")
    parser.add_argument(
        "--playwright-list",
        type=Path,
        help="JSON from `npx playwright test --list --reporter=json`; enables the domain-tag rule",
    )
    args = parser.parse_args(argv)

    problems = []
    sources = {}
    for spec in sorted(TESTS_ROOT.rglob("*.spec.ts")):
        source = spec.read_text(encoding="utf-8")
        rel = spec.relative_to(TESTS_ROOT).as_posix()
        sources[rel] = source
        problems.extend(
            str(v) for v in check_source(spec.relative_to(REPO_ROOT), source)
        )
        problems.extend(
            str(Violation(spec.relative_to(REPO_ROOT), line, message))
            for line, message in check_seed_features(rel, source, SEED_BORROWERS)
        )
    problems.extend(check_seed_borrowers(sources, SEED_BORROWERS))
    if args.playwright_list:
        problems.extend(
            check_domain_tags(
                json.loads(args.playwright_list.read_text(encoding="utf-8")),
                known_domains(TEST_TAGS.read_text(encoding="utf-8")),
            )
        )
    for problem in problems:
        print(problem)
    if problems:
        print(
            f"\n{len(problems)} P0/domain/seed tagging problem(s); see e2e-test/ui/playwright/utils/test-tags.ts.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
