#!/usr/bin/env python3
"""Fail if a p0 smoke test depends on a test that is not p0.

pytest-dependency SKIPS a test whose declared dependency did not run. Under the
PR tier (``-m p0``) a non-p0 dependency is deselected, so the dependent p0 test
skips and the batch still reports green -- the p0 marker buys nothing, and it
does so invisibly. This check fails loudly instead.

It is a pre-commit hook rather than a pytest test because the smoke-test suite
has a session-scoped autouse ``auth_session`` fixture that logs into the
frontend, so any test in that suite needs a running DataHub; this check is pure
static analysis and should cost nothing.

Limitation: only dependencies declared with ``@pytest.mark.dependency(depends=[...])``
are visible here. Tests that rely on ordering implicitly -- needing data another
test happened to create, with no ``depends=`` -- cannot be detected statically and
will only surface once the p0 tier runs in CI.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SMOKE_ROOT = REPO_ROOT / "smoke-test"
_DEPENDS = re.compile(r"depends=\[([^\]]*)\]", re.S)
_NAME = re.compile(r"name=['\"]([^'\"]+)['\"]")


def _test_modules() -> list[Path]:
    found = set(SMOKE_ROOT.rglob("test_*.py")) | set(SMOKE_ROOT.rglob("*_test.py"))
    return sorted(
        p for p in found if not {"venv", "build", "node_modules"} & set(p.parts)
    )


def _module_applies_p0(tree: ast.Module) -> bool:
    """True when a module-level pytestmark marks every test in the file p0."""
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "pytestmark"
            for target in node.targets
        ):
            if "mark.p0" in ast.unparse(node.value):
                return True
    return False


def _scan(path: Path) -> tuple[set[str], dict[str, list[str]], dict[str, str]]:
    """Return (p0 test names, {test: declared depends}, {alias: test})."""
    tree = ast.parse(path.read_text(errors="replace"))
    module_p0 = _module_applies_p0(tree)
    p0: set[str] = set()
    depends: dict[str, list[str]] = {}
    aliases: dict[str, str] = {}

    def visit(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, ast.ClassDef):
                visit(node.body)
                continue
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not node.name.startswith("test_"):
                continue
            decorators = " ".join(ast.unparse(d) for d in node.decorator_list)
            if module_p0 or "mark.p0" in decorators:
                p0.add(node.name)
            if "mark.dependency" in decorators:
                declared = _DEPENDS.search(decorators)
                if declared:
                    depends[node.name] = [
                        part.strip().strip("'\"")
                        for part in declared.group(1).split(",")
                        if part.strip()
                    ]
                alias = _NAME.search(decorators)
                if alias:
                    aliases[alias.group(1)] = node.name

    visit(tree.body)
    return p0, depends, aliases


def main() -> int:
    if not SMOKE_ROOT.is_dir():
        return 0
    gaps: list[str] = []
    for path in _test_modules():
        try:
            p0, depends, aliases = _scan(path)
        except SyntaxError:
            continue  # unparseable modules are caught by collection, not here
        if not p0 or not depends:
            continue
        rel = path.relative_to(REPO_ROOT)
        for test, declared in depends.items():
            if test not in p0:
                continue
            missing = [d for d in declared if aliases.get(d, d) not in p0]
            if missing:
                gaps.append(f"  {rel}::{test} -> depends on non-p0: {', '.join(missing)}")

    if gaps:
        print("p0 tests depend on tests that are not p0.\n")
        print("\n".join(gaps))
        print(
            "\npytest-dependency will SKIP these under `-m p0`, so the p0 marker has "
            "no effect and CI still goes green.\nMark the listed dependencies p0 as well."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
