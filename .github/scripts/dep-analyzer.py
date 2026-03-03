#!/usr/bin/env python3
"""
Unified dependency analyzer for Python projects.
Handles complex setup.py patterns like DataHub's dictionary-based dependencies.

Supports: requirements.txt, setup.py, pyproject.toml

License: MIT
Original code by: Claude (Anthropic) - no external source
"""
from __future__ import annotations

import ast
import sys
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterator

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet


class PinLevel(Enum):
    """Classification of version constraint strictness."""
    UNPINNED = "unpinned"
    LOWER_BOUND = "lower_bound"
    UPPER_BOUND = "upper_bound"
    RANGE = "range"
    COMPATIBLE = "compatible"
    WILDCARD = "wildcard"
    EXACT = "exact"


@dataclass
class DepAnalysis:
    """Analysis result for a single dependency."""
    name: str
    raw: str
    specifier: SpecifierSet
    level: PinLevel
    source: str


@dataclass
class AnalysisReport:
    """Full analysis report."""
    deps: list[DepAnalysis] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def by_level(self, level: PinLevel) -> list[DepAnalysis]:
        return [d for d in self.deps if d.level == level]

    @property
    def dynamic(self) -> list[DepAnalysis]:
        return [d for d in self.deps if d.level != PinLevel.EXACT]


def classify_specifier(spec: SpecifierSet) -> PinLevel:
    """Classify a specifier set by its constraint level."""
    specs = list(spec)
    if not specs:
        return PinLevel.UNPINNED

    operators = {s.operator for s in specs}

    if "==" in operators:
        for s in specs:
            if s.operator == "==" and "*" not in s.version:
                return PinLevel.EXACT
            if s.operator == "==" and "*" in s.version:
                return PinLevel.WILDCARD

    if "~=" in operators:
        return PinLevel.COMPATIBLE

    has_lower = operators & {">=", ">"}
    has_upper = operators & {"<=", "<", "!="}

    if has_lower and has_upper:
        return PinLevel.RANGE
    if has_lower:
        return PinLevel.LOWER_BOUND
    if has_upper:
        return PinLevel.UPPER_BOUND

    return PinLevel.UNPINNED


def parse_requirement(raw: str, source: str) -> DepAnalysis | None:
    """Parse a requirement string into analysis."""
    raw = raw.strip()
    if not raw or raw.startswith(("#", "-")):
        return None

    # Skip editable, git, URL-based deps
    if raw.startswith(("-e", "git+", "http://", "https://", "file:")):
        return None

    # Handle @ syntax (PEP 440 URL requirements)
    if " @ " in raw:
        return None

    try:
        req = Requirement(raw)
        return DepAnalysis(
            name=req.name,
            raw=raw,
            specifier=req.specifier,
            level=classify_specifier(req.specifier),
            source=source,
        )
    except Exception:
        return None


# -----------------------------------------------------------------------------
# requirements.txt parser
# -----------------------------------------------------------------------------

def parse_requirements_txt(path: Path) -> Iterator[DepAnalysis]:
    """Parse requirements.txt file."""
    for line in path.read_text().splitlines():
        line = line.split("#")[0].strip()  # Remove inline comments
        if dep := parse_requirement(line, str(path)):
            yield dep


# -----------------------------------------------------------------------------
# setup.py parser - handles complex patterns like DataHub
# -----------------------------------------------------------------------------

def extract_string_from_node(node: ast.expr) -> str | None:
    """Extract string value from AST node."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Str):  # Python 3.7 compat
        return node.s
    return None


def extract_strings_from_collection(node: ast.expr) -> list[str]:
    """Extract all string values from a list, set, or dict (keys for dict)."""
    strings = []
    if isinstance(node, (ast.List, ast.Set, ast.Tuple)):
        for elt in node.elts:
            if s := extract_string_from_node(elt):
                strings.append(s)
    elif isinstance(node, ast.Dict):
        # For dicts used as sets, keys are the dependencies
        for key in node.keys:
            if key and (s := extract_string_from_node(key)):
                strings.append(s)
    return strings


class SetupPyVisitor(ast.NodeVisitor):
    """AST visitor to extract dependencies from setup.py."""

    def __init__(self):
        self.variables: dict[str, list[str]] = {}
        self.install_requires: list[str] = []
        self.extras_require: dict[str, list[str]] = {}

    def visit_Assign(self, node: ast.Assign):
        """Capture variable assignments that look like dependency collections."""
        for target in node.targets:
            if isinstance(target, ast.Name):
                strings = extract_strings_from_collection(node.value)
                if strings:
                    self.variables[target.id] = strings
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        """Capture setup() or setuptools.setup() calls."""
        func_name = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name == "setup":
            for kw in node.keywords:
                if kw.arg == "install_requires":
                    self._extract_install_requires(kw.value)
                elif kw.arg == "extras_require":
                    self._extract_extras_require(kw.value)

        self.generic_visit(node)

    def _extract_install_requires(self, node: ast.expr):
        """Extract install_requires, handling list() wrapper and set operations."""
        # Handle list(something) wrapper
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "list":
            if node.args:
                node = node.args[0]

        # Handle set/dict operations (e.g., base_requirements | framework_common)
        if isinstance(node, ast.BinOp):
            self._extract_from_binop(node, self.install_requires)
        else:
            # Direct list/set
            strings = extract_strings_from_collection(node)
            if strings:
                self.install_requires.extend(strings)
            # Variable reference
            elif isinstance(node, ast.Name) and node.id in self.variables:
                self.install_requires.extend(self.variables[node.id])

    def _extract_from_binop(self, node: ast.BinOp, target: list[str]):
        """Recursively extract from binary operations (set unions)."""
        for operand in (node.left, node.right):
            if isinstance(operand, ast.BinOp):
                self._extract_from_binop(operand, target)
            elif isinstance(operand, ast.Name) and operand.id in self.variables:
                target.extend(self.variables[operand.id])
            else:
                strings = extract_strings_from_collection(operand)
                target.extend(strings)

    def _extract_extras_require(self, node: ast.expr):
        """Extract extras_require dict."""
        if not isinstance(node, ast.Dict):
            return

        for key, value in zip(node.keys, node.values):
            if key is None:
                continue
            extra_name = extract_string_from_node(key)
            if not extra_name:
                continue

            deps = []
            # Handle list(something) wrapper
            if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "list":
                if value.args:
                    value = value.args[0]

            if isinstance(value, ast.BinOp):
                self._extract_from_binop(value, deps)
            elif isinstance(value, (ast.List, ast.Set)):
                deps.extend(extract_strings_from_collection(value))
            elif isinstance(value, ast.Name) and value.id in self.variables:
                deps.extend(self.variables[value.id])

            if deps:
                self.extras_require[extra_name] = deps


def parse_setup_py_ast(path: Path) -> Iterator[DepAnalysis]:
    """Parse setup.py using AST analysis (safe, no execution)."""
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError as e:
        print(f"Warning: Syntax error in {path}: {e}", file=sys.stderr)
        return

    visitor = SetupPyVisitor()
    visitor.visit(tree)

    source = str(path)
    seen = set()

    for dep in visitor.install_requires:
        if dep not in seen:
            seen.add(dep)
            if analysis := parse_requirement(dep, f"{source}:install_requires"):
                yield analysis

    for extra, deps in visitor.extras_require.items():
        for dep in deps:
            key = (dep, extra)
            if key not in seen:
                seen.add(key)
                if analysis := parse_requirement(dep, f"{source}:extras_require[{extra}]"):
                    yield analysis


def parse_setup_py_exec(path: Path) -> Iterator[DepAnalysis]:
    """
    Parse setup.py by executing it and capturing setup() arguments.
    More reliable for complex setups but requires execution.
    """
    import tempfile
    import importlib.util
    from unittest.mock import patch

    collected_install: list[str] = []
    collected_extras: dict[str, list[str]] = {}

    def mock_setup(*args, **kwargs):
        if "install_requires" in kwargs:
            reqs = kwargs["install_requires"]
            if reqs:
                collected_install.extend(reqs if isinstance(reqs, list) else list(reqs))
        if "extras_require" in kwargs:
            extras = kwargs["extras_require"]
            if extras:
                for k, v in extras.items():
                    collected_extras[k] = list(v) if v else []

    original_cwd = os.getcwd()
    try:
        os.chdir(path.parent)

        # Create a fake module context
        fake_globals = {
            "__name__": "__main__",
            "__file__": str(path.name),
            "__builtins__": __builtins__,
        }

        with patch("setuptools.setup", mock_setup):
            with patch("distutils.core.setup", mock_setup):
                try:
                    exec(compile(path.read_text(), path.name, "exec"), fake_globals)
                except SystemExit:
                    pass  # setup() sometimes calls sys.exit()
                except Exception as e:
                    print(f"Warning: Error executing {path}: {e}", file=sys.stderr)

    finally:
        os.chdir(original_cwd)

    source = str(path)
    seen = set()

    for dep in collected_install:
        if dep not in seen:
            seen.add(dep)
            if analysis := parse_requirement(dep, f"{source}:install_requires"):
                yield analysis

    for extra, deps in collected_extras.items():
        for dep in deps:
            key = (dep, extra)
            if key not in seen:
                seen.add(key)
                if analysis := parse_requirement(dep, f"{source}:extras_require[{extra}]"):
                    yield analysis


# -----------------------------------------------------------------------------
# pyproject.toml parser
# -----------------------------------------------------------------------------

def parse_pyproject_toml(path: Path) -> Iterator[DepAnalysis]:
    """Parse pyproject.toml (PEP 621 and setuptools-specific)."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            print("Warning: tomllib/tomli not available, skipping pyproject.toml", file=sys.stderr)
            return

    try:
        data = tomllib.loads(path.read_text())
    except Exception as e:
        print(f"Warning: Error parsing {path}: {e}", file=sys.stderr)
        return

    source = str(path)

    # PEP 621: project.dependencies
    for dep in data.get("project", {}).get("dependencies", []):
        if analysis := parse_requirement(dep, f"{source}:project.dependencies"):
            yield analysis

    # PEP 621: project.optional-dependencies
    for extra, deps in data.get("project", {}).get("optional-dependencies", {}).items():
        for dep in deps:
            if analysis := parse_requirement(dep, f"{source}:project.optional-dependencies[{extra}]"):
                yield analysis

    # Poetry: tool.poetry.dependencies
    poetry_deps = data.get("tool", {}).get("poetry", {}).get("dependencies", {})
    for name, spec in poetry_deps.items():
        if name == "python":
            continue
        dep_str = _poetry_spec_to_pep508(name, spec)
        if dep_str and (analysis := parse_requirement(dep_str, f"{source}:tool.poetry.dependencies")):
            yield analysis

    # Poetry: tool.poetry.group.*.dependencies
    for group_name, group in data.get("tool", {}).get("poetry", {}).get("group", {}).items():
        for name, spec in group.get("dependencies", {}).items():
            dep_str = _poetry_spec_to_pep508(name, spec)
            if dep_str and (analysis := parse_requirement(dep_str, f"{source}:tool.poetry.group.{group_name}")):
                yield analysis


def _poetry_spec_to_pep508(name: str, spec) -> str | None:
    """Convert Poetry dependency spec to PEP 508 string."""
    if isinstance(spec, str):
        if spec == "*":
            return name
        # Poetry uses ^, ~ differently; approximate conversion
        if spec.startswith("^"):
            return f"{name}>={spec[1:]}"
        if spec.startswith("~"):
            return f"{name}~={spec[1:]}"
        return f"{name}{spec}"
    elif isinstance(spec, dict):
        version = spec.get("version", "*")
        if version == "*":
            return name
        if version.startswith("^"):
            return f"{name}>={version[1:]}"
        return f"{name}{version}"
    return name


# -----------------------------------------------------------------------------
# Main analyzer
# -----------------------------------------------------------------------------

def analyze_project(
    project_dir: str | Path = ".",
    use_exec: bool = False,
) -> AnalysisReport:
    """
    Analyze all dependency sources in a project.

    Args:
        project_dir: Path to project root
        use_exec: If True, execute setup.py (more accurate but requires deps)
    """
    project_dir = Path(project_dir).resolve()
    report = AnalysisReport()
    seen: set[tuple[str, str]] = set()

    def add_deps(deps: Iterator[DepAnalysis]):
        for dep in deps:
            key = (dep.name.lower(), dep.source)
            if key not in seen:
                seen.add(key)
                report.deps.append(dep)

    # requirements*.txt files
    for pattern in ["requirements.txt", "requirements*.txt", "requirements/*.txt"]:
        for req_file in project_dir.glob(pattern):
            if req_file.is_file():
                add_deps(parse_requirements_txt(req_file))

    # setup.py
    setup_py = project_dir / "setup.py"
    if setup_py.exists():
        if use_exec:
            add_deps(parse_setup_py_exec(setup_py))
        else:
            add_deps(parse_setup_py_ast(setup_py))

    # pyproject.toml
    pyproject = project_dir / "pyproject.toml"
    if pyproject.exists():
        add_deps(parse_pyproject_toml(pyproject))

    return report


def print_report(report: AnalysisReport, verbose: bool = False):
    """Print formatted analysis report."""
    print("=" * 70)
    print("DEPENDENCY VERSION ANALYSIS")
    print("=" * 70)

    level_order = [
        PinLevel.UNPINNED,
        PinLevel.LOWER_BOUND,
        PinLevel.UPPER_BOUND,
        PinLevel.RANGE,
        PinLevel.COMPATIBLE,
        PinLevel.WILDCARD,
        PinLevel.EXACT,
    ]

    for level in level_order:
        deps = report.by_level(level)
        if not deps:
            continue

        icon = "✓" if level == PinLevel.EXACT else "⚠"
        print(f"\n{icon} {level.value.upper()} ({len(deps)})")
        print("-" * 60)

        for dep in sorted(deps, key=lambda d: d.name.lower()):
            spec = str(dep.specifier) if dep.specifier else "(any)"
            if verbose:
                print(f"  {dep.name:<25} {spec:<20}")
                print(f"    └─ {dep.source}")
            else:
                print(f"  {dep.name:<25} {spec:<20} [{dep.source.split(':')[-1]}]")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    dynamic = report.dynamic
    total = len(report.deps)
    exact = len(report.by_level(PinLevel.EXACT))
    unpinned = len(report.by_level(PinLevel.UNPINNED))

    print(f"  Total dependencies:        {total}")
    print(f"  Exactly pinned (==x.y.z):  {exact} ({100*exact//total if total else 0}%)")
    print(f"  Completely unpinned:       {unpinned}")
    print(f"  Will resolve dynamically:  {len(dynamic)}")

    if unpinned:
        print(f"\n⚠ UNPINNED dependencies (highest risk):")
        for dep in report.by_level(PinLevel.UNPINNED):
            print(f"    - {dep.name}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Analyze Python project dependencies for unpinned versions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s .                    Analyze current directory
  %(prog)s /path/to/project     Analyze specific project
  %(prog)s . --exec             Execute setup.py (more accurate)
  %(prog)s . --verbose          Show full source paths
  %(prog)s . --json             Output as JSON
        """,
    )
    parser.add_argument("project_dir", nargs="?", default=".", help="Project directory")
    parser.add_argument("--exec", action="store_true", help="Execute setup.py instead of AST parsing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--fail-unpinned", action="store_true", help="Exit 1 if any unpinned deps")
    parser.add_argument("--fail-dynamic", action="store_true", help="Exit 1 if any non-exact pins")

    args = parser.parse_args()

    report = analyze_project(args.project_dir, use_exec=args.exec)

    if args.json:
        import json
        output = {
            "total": len(report.deps),
            "by_level": {
                level.value: [
                    {"name": d.name, "specifier": str(d.specifier), "source": d.source}
                    for d in report.by_level(level)
                ]
                for level in PinLevel
            },
        }
        print(json.dumps(output, indent=2))
    else:
        print_report(report, verbose=args.verbose)

    # Exit codes for CI
    if args.fail_unpinned and report.by_level(PinLevel.UNPINNED):
        sys.exit(1)
    if args.fail_dynamic and report.dynamic:
        sys.exit(1)


if __name__ == "__main__":
    main()
