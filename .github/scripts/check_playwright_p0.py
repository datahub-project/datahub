#!/usr/bin/env python3
"""Fail when a Playwright P0 tag sits on a single test inside a serial spec.

P0 tests are selected with ``--grep @p0``. In a ``mode: 'serial'`` describe,
later tests depend on state the earlier ones created (see
tests/incidents-v2/v2-incidents.spec.ts: create, then update the same
incident). Tagging one test of such a chain makes a P0 run execute it without
its predecessors, so it fails, or worse, passes against leftover state.
Tagging the describe instead selects the whole chain.

Static and stdlib-only, like check_p0_dependencies.py, so the pre-commit hook
runs it in seconds without Node. It is deliberately conservative: in a file
that turns on serial mode anywhere, every P0 tag must sit on a
``test.describe`` call.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS_ROOT = REPO_ROOT / "e2e-test" / "ui" / "playwright" / "tests"

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


def main() -> int:
    problems = [
        str(violation)
        for spec in sorted(TESTS_ROOT.rglob("*.spec.ts"))
        for violation in check_source(
            spec.relative_to(REPO_ROOT), spec.read_text(encoding="utf-8")
        )
    ]
    for problem in problems:
        print(problem)
    if problems:
        print(
            f"\n{len(problems)} P0 tagging problem(s); see e2e-test/ui/playwright/utils/test-tags.ts.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
