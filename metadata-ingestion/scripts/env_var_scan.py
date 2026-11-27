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

    writes: Dict[str, List[str]] = field(default_factory=dict)
    """Variable name -> sites that SET it. Setting a variable for a subprocess is not the SDK
    reading its own configuration, so these are kept apart from reads."""

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


def _module_string_constants(tree: ast.Module) -> Dict[str, str]:
    """Module-level ``NAME = "LITERAL"`` bindings.

    Read sites often pass a constant rather than a literal (``os.environ.get(REGISTRY_URL_ENV)``),
    which plain literal-matching misses.
    """
    constants: Dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        constants[target.id] = node.value.value
    return constants


def _env_aliases(tree: ast.Module) -> Dict[str, str]:
    """Names bound by ``from os import getenv, environ``, which hide the ``os.`` prefix."""
    aliases: Dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "os":
            for alias in node.names:
                if alias.name in ("getenv", "environ"):
                    aliases[alias.asname or alias.name] = alias.name
    return aliases


def _name_of(node: ast.expr, constants: Dict[str, str]) -> str:
    """The variable name an argument denotes, literal or via a module constant."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    return ""


def _is_environ(node: ast.expr, aliases: Dict[str, str]) -> bool:
    """``os.environ``, or a bare ``environ`` imported from os."""
    if isinstance(node, ast.Attribute) and node.attr == "environ":
        return True
    return isinstance(node, ast.Name) and aliases.get(node.id) == "environ"


def _is_env_read(node: ast.Call, aliases: Dict[str, str]) -> bool:
    """Whether this call reads an environment variable named by its first argument."""
    func = node.func
    if isinstance(func, ast.Name):
        # A helper such as get_boolean_env_variable, or a bare getenv imported from os.
        return func.id in _READ_HELPERS or aliases.get(func.id) == "getenv"
    if isinstance(func, ast.Attribute):
        if func.attr in _READ_HELPERS:
            return True
        if func.attr == "getenv":
            return isinstance(func.value, ast.Name) and func.value.id == "os"
        # environ.get(...) and environ.setdefault(...), with or without the os. prefix.
        if func.attr in ("get", "setdefault"):
            return _is_environ(func.value, aliases)
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

        constants = _module_string_constants(tree)
        aliases = _env_aliases(tree)

        for node in ast.walk(tree):
            name = ""
            is_write = False
            if id(node) in declared_subtrees:
                continue
            if isinstance(node, ast.Call):
                if _is_env_read(node, aliases) and node.args:
                    name = _name_of(node.args[0], constants)
            elif isinstance(node, ast.Subscript):
                if _is_environ(node.value, aliases):
                    name = _name_of(node.slice, constants)
                    # os.environ["X"] = ... sets a variable for a child process; it is not
                    # the SDK reading its own configuration.
                    is_write = isinstance(node.ctx, (ast.Store, ast.Del))
            elif isinstance(node, ast.Compare):
                # "X" in os.environ -- a presence check is a read.
                if any(isinstance(op, (ast.In, ast.NotIn)) for op in node.ops) and any(
                    _is_environ(cmp, aliases) for cmp in node.comparators
                ):
                    name = _name_of(node.left, constants)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if _OWNED_NAME.match(node.value):
                    name = node.value

            if name and is_write:
                lineno = getattr(node, "lineno", 0)
                result.writes.setdefault(name, []).append(f"{rel}:{lineno}")
                continue

            if name:
                lineno = getattr(node, "lineno", 0)
                result.reads.setdefault(name, []).append(f"{rel}:{lineno}")

    return result


def default_source_root() -> pathlib.Path:
    """``src/datahub`` relative to this script, however the caller was invoked."""
    return pathlib.Path(__file__).resolve().parent.parent / "src" / "datahub"
