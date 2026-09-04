#!/usr/bin/env python3
"""Fail if a p0 smoke test depends on a test that is not p0.

pytest-dependency SKIPS a test whose declared dependency did not run. When the
PR tier selects only ``p0`` tests, a non-p0 dependency is deselected, so the
dependent p0 test skips and the batch still reports green -- the p0 marker buys
nothing, and it does so invisibly. This check fails loudly instead.

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
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SMOKE_ROOT = REPO_ROOT / "smoke-test"


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


def _dependency_kwargs(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[list[str] | None, str | None]:
    """Read ``depends=`` and ``name=`` off a ``@pytest.mark.dependency`` decorator.

    Taken from the AST rather than by regex. A declared dependency may name a
    parametrized instance (``test_foo[case]``), and a regex bounded by the first
    ``]`` truncates the list at that inner bracket -- silently discarding every
    later entry and leaving a malformed name that can never match a test.
    """
    depends: list[str] | None = None
    alias: str | None = None
    for dec in node.decorator_list:
        if not isinstance(dec, ast.Call):
            continue
        if not ast.unparse(dec.func).endswith("mark.dependency"):
            continue
        for kw in dec.keywords:
            if kw.arg == "depends" and isinstance(kw.value, (ast.List, ast.Tuple)):
                depends = [
                    elt.value
                    for elt in kw.value.elts
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                ]
            elif (
                kw.arg == "name"
                and isinstance(kw.value, ast.Constant)
                and isinstance(kw.value.value, str)
            ):
                alias = kw.value.value
    return depends, alias


def _dependency_target(dep: str) -> str:
    """The test function a declared dependency refers to.

    pytest-dependency accepts a bare name, a parametrized instance
    (``test_foo[case]``) or a node id (``TestClass::test_foo``). The p0 marker
    attaches to the function, so all three forms resolve to the function name.
    """
    return dep.split("::")[-1].split("[", 1)[0]


def _resolves_to_p0(dep: str, aliases: dict[str, str], p0: set[str]) -> bool:
    """True when a declared dependency names a p0 test, directly or by alias."""
    for candidate in (dep, _dependency_target(dep)):
        if aliases.get(candidate, candidate) in p0:
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
            declared, alias = _dependency_kwargs(node)
            if declared is not None:
                depends[node.name] = declared
            if alias:
                aliases[alias] = node.name

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
            missing = [d for d in declared if not _resolves_to_p0(d, aliases, p0)]
            if missing:
                gaps.append(f"  {rel}::{test} -> depends on non-p0: {', '.join(missing)}")

    if gaps:
        print("p0 tests depend on tests that are not p0.\n")
        print("\n".join(gaps))
        print(
            "\npytest-dependency will SKIP these when only p0 runs, so the p0 marker has "
            "no effect and CI still goes green.\nMark the listed dependencies p0 as well."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
