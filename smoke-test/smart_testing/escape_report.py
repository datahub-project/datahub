"""Report post-merge escapes: tests that failed after merge but never ran on the PR.

Under active smart testing a PR runs only its planned scopes, so a skipped test
that the change breaks first fails in the post-merge (push) run. This compares
that run's final failures with what the merged PR's last run ran: the testcase
classnames in its junit, plus the scopes in its batch plan (which covers a PR
batch whose junit is missing). A failure whose classname is in neither is an
escape.

A junit ``classname`` is the pytest nodeid without its last ``::`` segment,
with ``.py`` dropped and ``/`` turned into ``.``. That is the plan's loadscope
key in dotted form, so the match is exact. A module-level skip or collection
error has an empty ``classname`` and the dotted module in ``name``; it counts as
run on the PR when any scope of that module ran.

Every active plan is checked, including the p0 fallback (no smart tags), whose
escapes are failures outside the p0 tier. A shadow plan ran the fixed matrix
and has its own comparison (shadow_report), so it is not checked.

Usage:
    python -m smart_testing.escape_report --plan PLAN \
        --post-merge-junit-dir DIR --pr-junit-dir DIR --out RESULT
Writes RESULT (JSON) when the check applies, else prints why to stderr. Exit
code is always 0. Stdlib only, so CI can run it with any python3.
"""

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from smart_testing.shadow_report import final_cases, is_final_failure


@dataclass
class EscapeCheck:
    post_merge_failures: List[str]
    escapes: List[str]
    smart_tags_applied: bool
    tests_selected: Optional[int]


def scope_classname(scope: str) -> str:
    """``tests/e2e/a.py::TestX`` -> ``tests.e2e.a.TestX``, as pytest's junit writes it."""
    path, _, rest = scope.partition("::")
    module = re.sub(r"\.py$", "", path).replace("/", ".")
    return ".".join([module, *rest.split("::")]) if rest else module


def _scope(case: ET.Element) -> str:
    return case.get("classname") or case.get("name") or ""


def _with_parents(scopes: Iterable[str]) -> Set[str]:
    """*scopes* plus each dotted prefix, so a module matches its class scopes."""
    parts = (scope.split(".") for scope in scopes)
    return {".".join(p[:i]) for p in parts for i in range(1, len(p) + 1)}


def check(
    plan: Dict[str, Any],
    post_merge_junit: Iterable[str],
    pr_junit: Iterable[str],
) -> Optional[EscapeCheck]:
    """The escape check, or None when the PR's plan was not applied (shadow)."""
    if plan.get("smart_mode") != "active":
        return None
    ran_on_pr = _with_parents(
        [_scope(case) for case in final_cases(pr_junit).values()]
        + [
            scope_classname(scope)
            for batch in plan.get("batches", [])
            for scope in batch.get("scopes", [])
        ]
    )
    failures = {
        name: case
        for name, case in final_cases(post_merge_junit).items()
        if is_final_failure(case)
    }
    test_count = plan.get("test_count")
    return EscapeCheck(
        post_merge_failures=sorted(failures),
        escapes=sorted(
            name for name, case in failures.items() if _scope(case) not in ran_on_pr
        ),
        smart_tags_applied=bool(plan.get("smart_tags_applied")),
        tests_selected=test_count if isinstance(test_count, int) else None,
    )


def _junit_files(directory: Path) -> List[str]:
    return sorted(str(path) for path in directory.rglob("junit.*.xml"))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--post-merge-junit-dir", type=Path, required=True)
    parser.add_argument("--pr-junit-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    post_merge_junit = _junit_files(args.post_merge_junit_dir)
    if not post_merge_junit:
        print("[smart-testing] no post-merge pytest junit; skipped", file=sys.stderr)
        return 0
    try:
        plan = json.loads(args.plan.read_text())
    except (OSError, ValueError) as exc:
        print(f"[smart-testing] no usable PR plan ({exc}); skipped", file=sys.stderr)
        return 0
    result = check(plan, post_merge_junit, _junit_files(args.pr_junit_dir))
    if result is None:
        print(
            f"[smart-testing] PR plan smart_mode={plan.get('smart_mode')!r} "
            "was not applied; skipped",
            file=sys.stderr,
        )
        return 0
    args.out.write_text(json.dumps(asdict(result), sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
