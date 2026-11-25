#!/usr/bin/env python3
"""Fail when a smoke test module uses a backend capability it does not declare.

The Docker-free local stack has no Kafka, no SQL metadata database and no
docker CLI. ``smoke-test/utilities/backend_plugin.py`` deselects tests marked
``@pytest.mark.requires(...)`` for what the local stack lacks, so a test that
needs one of these but carries no marker would run on the local lane and fail
there. This check finds the clear cases before CI does:

- importing ``confluent_kafka`` or ``DatahubKafkaEmitter``, or calling
  ``get_kafka_broker_url`` / ``get_kafka_schema_registry`` => ``kafka``
- calling ``get_db_type`` / ``get_db_url`` / ``get_db_username`` /
  ``get_db_password`` => ``sql_db``
- running ``subprocess`` with ``"docker"`` as the program => ``docker``

A module satisfies a capability when any ``requires(...)`` in it names that
capability: on the module, a class, a test or a ``pytest.param``. That is
deliberately per module: helpers inside a module are shared by its tests, so
the check cannot tell which test reaches them. It also does not follow calls
into other modules; the getters' runtime check (``require_capability``)
covers those.

It also checks that every capability named in ``requires(...)`` exists in the
``Capability`` enum and that every ``docker_canary`` test is ``p0``, since the
Docker canary lane runs the p0 tier.

Static and stdlib-only, like check_p0_dependencies.py, so the pre-commit hook
runs it in well under a second.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
SMOKE_ROOT = REPO_ROOT / "smoke-test"
E2E_ROOT = SMOKE_ROOT / "tests" / "e2e"
BACKENDS = SMOKE_ROOT / "utilities" / "backends.py"

_KAFKA_IMPORTS = {"confluent_kafka", "DatahubKafkaEmitter"}
_CALL_SIGNALS = {
    "get_kafka_broker_url": "kafka",
    "get_kafka_schema_registry": "kafka",
    "get_db_type": "sql_db",
    "get_db_url": "sql_db",
    "get_db_username": "sql_db",
    "get_db_password": "sql_db",
}
_SUBPROCESS_CALLS = {"run", "Popen", "call", "check_call", "check_output"}


def load_capabilities(backends: Path = BACKENDS) -> dict[str, str]:
    """``{enum member name: value}`` of ``Capability``, read without importing it."""
    tree = ast.parse(backends.read_text())
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "Capability":
            return {
                stmt.targets[0].id: stmt.value.value
                for stmt in node.body
                if isinstance(stmt, ast.Assign)
                and isinstance(stmt.targets[0], ast.Name)
                and isinstance(stmt.value, ast.Constant)
            }
    raise ValueError(f"no Capability enum in {backends}")


def _call_name(func: ast.expr) -> Optional[str]:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _is_mark(node: ast.expr, name: str) -> bool:
    """True for ``pytest.mark.<name>`` and ``mark.<name>``, called or not."""
    target = node.func if isinstance(node, ast.Call) else node
    return isinstance(target, ast.Attribute) and target.attr == name and (
        ast.unparse(target.value).endswith("mark")
    )


def _runs_docker(call: ast.Call) -> bool:
    func = call.func
    if not (
        isinstance(func, ast.Attribute)
        and func.attr in _SUBPROCESS_CALLS
        and ast.unparse(func.value) == "subprocess"
        and call.args
    ):
        return False
    program = call.args[0]
    if isinstance(program, (ast.List, ast.Tuple)) and program.elts:
        program = program.elts[0]
    return isinstance(program, ast.Constant) and (
        program.value == "docker" or str(program.value).startswith("docker ")
    )


def _required_capabilities(
    call: ast.Call, capabilities: dict[str, str]
) -> tuple[set[str], list[str]]:
    """(capability values named by a requires(...) call, unknown names)."""
    known_values = set(capabilities.values())
    named: set[str] = set()
    unknown: list[str] = []
    for arg in call.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            value = arg.value.strip().lower()
            (named.add(value) if value in known_values else unknown.append(arg.value))
        elif isinstance(arg, ast.Attribute) and ast.unparse(arg.value) == "Capability":
            if arg.attr in capabilities:
                named.add(capabilities[arg.attr])
            else:
                unknown.append(f"Capability.{arg.attr}")
        else:
            unknown.append(ast.unparse(arg))
    return named, unknown


def _marks_p0(decorators: Iterable[ast.expr]) -> bool:
    return any(_is_mark(d, "p0") for d in decorators)


def _pytestmark(body: list[ast.stmt]) -> list[ast.expr]:
    for node in body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "pytestmark" for t in node.targets
        ):
            value = node.value
            return list(value.elts) if isinstance(value, (ast.List, ast.Tuple)) else [value]
    return []


def _canary_gaps(tree: ast.Module) -> list[str]:
    """Names of docker_canary tests (or ``<module>``) that are not p0."""
    gaps: list[str] = []

    def visit(body: list[ast.stmt], inherited: list[ast.expr], scope: str) -> None:
        marks = inherited + _pytestmark(body)
        if any(_is_mark(m, "docker_canary") for m in marks) and not _marks_p0(marks):
            gaps.append(scope)
            return
        for node in body:
            if isinstance(node, ast.ClassDef):
                visit(node.body, marks + node.decorator_list, node.name)
            elif isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef)
            ) and node.name.startswith("test_"):
                own = marks + node.decorator_list
                if any(_is_mark(d, "docker_canary") for d in node.decorator_list) and not (
                    _marks_p0(own)
                ):
                    gaps.append(node.name)

    visit(tree.body, [], "<module>")
    return gaps


def check_source(source: str, capabilities: dict[str, str]) -> list[str]:
    """Problems found in one test module's source; empty when it is fine."""
    tree = ast.parse(source)
    used: dict[str, str] = {}
    declared: set[str] = set()
    problems: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in _KAFKA_IMPORTS:
                    used.setdefault("kafka", f"imports {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            module = (node.module or "").split(".")[0]
            names = {alias.name for alias in node.names}
            hit = ({module} | names) & _KAFKA_IMPORTS
            if hit:
                used.setdefault("kafka", f"imports {sorted(hit)[0]}")
        elif isinstance(node, ast.Call):
            name = _call_name(node.func)
            if name in _CALL_SIGNALS:
                used.setdefault(_CALL_SIGNALS[name], f"calls {name}()")
            elif _runs_docker(node):
                used.setdefault("docker", "runs the docker CLI")
            elif _is_mark(node, "requires"):
                named, unknown = _required_capabilities(node, capabilities)
                declared |= named
                problems += [
                    f"requires(...) names unknown capability {name!r}" for name in unknown
                ]

    for capability, reason in sorted(used.items()):
        if capability not in declared:
            problems.append(
                f"{reason} but no requires(...) in the module names {capability!r}"
            )
    problems += [
        f"{scope} is docker_canary but not p0" for scope in _canary_gaps(tree)
    ]
    return problems


def _test_modules() -> list[Path]:
    found = set(E2E_ROOT.rglob("test_*.py")) | set(E2E_ROOT.rglob("*_test.py"))
    return sorted(p for p in found if "__pycache__" not in p.parts)


def main() -> int:
    if not E2E_ROOT.is_dir():
        return 0
    capabilities = load_capabilities()
    report: list[str] = []
    for path in _test_modules():
        try:
            problems = check_source(path.read_text(errors="replace"), capabilities)
        except SyntaxError:
            continue  # unparseable modules are caught by collection, not here
        rel = path.relative_to(REPO_ROOT)
        report += [f"  {rel}: {problem}" for problem in problems]

    if report:
        print("Smoke test backend markers are missing or wrong.\n")
        print("\n".join(report))
        print(
            "\nAdd pytest.mark.requires(Capability.<NAME>) from utilities/backends.py to the "
            "test, its class or the module's pytestmark. Without it the Docker-free local "
            "lane runs the test and it fails there. See 'Backend capabilities' in "
            "smoke-test/AGENTS.md."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
