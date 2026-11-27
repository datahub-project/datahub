"""Static discovery of environment variable reads and declarations in the datahub package.

Both the drift test and the docs generator rely on this, so that "what the code reads" and
"what the docs describe" are measured the same way. It is deliberately static: the
declarations live wherever the read lives, and importing every such module just to find them
would make discovery depend on which optional extras happen to be installed.
"""

import ast
import pathlib
import re
from dataclasses import dataclass, field
from typing import Dict, List, Set

# Helpers that take the variable name as their first argument. Reads routed through these
# are just as real as a bare os.getenv, and were the ones the registry kept missing.
_READ_HELPERS = {
    "get_boolean_env_variable",
    "parse_bool_env",
    "string_to_bool",
    "_get_int_env",
}

# A DataHub-owned name is a read wherever it appears -- including when it is first bound to a
# module constant and only later passed to os.environ.get, which plain call-matching misses.
_OWNED_NAME = re.compile(
    r"^(DATAHUB|ACRYL|INGESTION|EXECUTOR|UI_INGESTION)_[A-Z0-9_]+$"
)

_DECLARATORS = {"env_var", "declare_env_var", "EnvVarSpec"}


@dataclass
class ScanResult:
    reads: Dict[str, List[str]] = field(default_factory=dict)
    """Variable name -> "path:line" for every site that reads it."""

    declared: Dict[str, str] = field(default_factory=dict)
    """Variable name -> "path:line" of its declaration."""

    declaring_modules: Set[str] = field(default_factory=set)
    """Dotted module names that declare at least one variable."""


def _keyword_str(node: ast.Call, name: str) -> str:
    for kw in node.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant):
            value = kw.value.value
            if isinstance(value, str):
                return value
    return ""


def _is_env_read(node: ast.Call) -> bool:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id in _READ_HELPERS
    if isinstance(func, ast.Attribute):
        if func.attr in _READ_HELPERS:
            return True
        if func.attr == "getenv":
            return isinstance(func.value, ast.Name) and func.value.id == "os"
        if func.attr == "get":
            return (
                isinstance(func.value, ast.Attribute) and func.value.attr == "environ"
            )
    return False


def scan_tree(root: pathlib.Path, package: str = "datahub") -> ScanResult:
    """Walk ``root`` for environment variable reads and declarations."""
    result = ScanResult()

    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError) as exc:
            # Skipping silently would quietly shrink the completeness check: a file that
            # cannot be parsed is a file whose env var reads nobody is checking.
            raise RuntimeError(
                f"Could not parse {path} while scanning for env vars"
            ) from exc

        rel = path.relative_to(root.parent)
        module = ".".join(rel.with_suffix("").parts)
        if module.endswith(".__init__"):
            module = module[: -len(".__init__")]

        # A declaration spells its variable name as a string literal, which would otherwise
        # register as a read of itself. Skip everything inside a declaration call.
        declared_subtrees: Set[int] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                callee = (
                    func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
                )
                if callee in _DECLARATORS:
                    declared_name = _keyword_str(node, "name")
                    if declared_name:
                        result.declared.setdefault(
                            declared_name, f"{rel}:{node.lineno}"
                        )
                        result.declaring_modules.add(module)
                    declared_subtrees.update(id(child) for child in ast.walk(node))

        for node in ast.walk(tree):
            name = ""
            if id(node) in declared_subtrees:
                continue
            if isinstance(node, ast.Call):
                if _is_env_read(node) and node.args:
                    first = node.args[0]
                    if isinstance(first, ast.Constant) and isinstance(first.value, str):
                        name = first.value
            elif isinstance(node, ast.Subscript):
                value = node.value
                if (
                    isinstance(value, ast.Attribute)
                    and value.attr == "environ"
                    and isinstance(node.slice, ast.Constant)
                    and isinstance(node.slice.value, str)
                ):
                    name = node.slice.value
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if _OWNED_NAME.match(node.value):
                    name = node.value

            if name:
                lineno = getattr(node, "lineno", 0)
                result.reads.setdefault(name, []).append(f"{rel}:{lineno}")

    return result


def default_source_root() -> pathlib.Path:
    """``src/datahub`` relative to this script, however the caller was invoked."""
    return pathlib.Path(__file__).resolve().parent.parent / "src" / "datahub"
