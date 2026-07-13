"""
Force pure-Python loading of sqlglot when DATAHUB_SQLGLOT_DISABLE_C is set.

sqlglot[c] ships mypyc-compiled .so extensions for a performance boost. These
native C extensions have caused memory leaks in the past, severe enough that we
once had to drop the dependency from a release. Rather than dropping it again,
this hook offers an alternative: force the native C lib to be ignored
dynamically at runtime, per-process, without removing the dependency from the
release or changing the installed packages.

Python's import machinery hardcodes .so priority over .py, with no built-in
knob to flip it. So when the env var is set we install a sys.meta_path finder
that resolves sqlglot.* imports to their .py source files, bypassing the .so.

This runs at interpreter startup via datahub_force_pure_python_sqlglot.pth,
before any application or dependency code can import sqlglot. It reads the env
var directly with os.getenv rather than importing
datahub.configuration.env_vars, because the full datahub package is not yet
importable this early in startup.

Manual check:
    DATAHUB_SQLGLOT_DISABLE_C=1 python -c "from sqlglot import tokenizer_core; print(tokenizer_core.__file__)"

    Should print a path ending in .py (not .so).
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec
from types import ModuleType
from typing import Optional, Sequence

logger = logging.getLogger(__name__)


class _PurePythonSqlglotFinder(MetaPathFinder):
    """Resolves sqlglot.* imports to .py source files, bypassing .so extensions."""

    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[str]],
        target: Optional[ModuleType] = None,
    ) -> Optional[ModuleSpec]:
        if fullname != "sqlglot" and not fullname.startswith("sqlglot."):
            return None

        # `path` is the parent package's __path__ for submodules, or None for the
        # top-level sqlglot import (in which case we fall back to sys.path).
        search_paths = path if path is not None else sys.path
        leaf = fullname.rpartition(".")[2]

        for entry in search_paths:
            if not isinstance(entry, str):
                continue
            base = os.path.join(entry, leaf)

            package_init = os.path.join(base, "__init__.py")
            if os.path.isfile(package_init):
                return importlib.util.spec_from_file_location(
                    fullname,
                    package_init,
                    submodule_search_locations=[base],
                )

            module_source = base + ".py"
            if os.path.isfile(module_source):
                return importlib.util.spec_from_file_location(fullname, module_source)

        return None


def _sqlglot_c_disabled() -> bool:
    # This env var is deliberately read here and NOT registered in
    # datahub.configuration.env_vars: this module is the only consumer, and it
    # runs at interpreter startup (via .pth) before env_vars — which pulls in
    # pydantic and the rest of the datahub package — can be safely imported.
    return os.getenv("DATAHUB_SQLGLOT_DISABLE_C", "").strip().lower() not in (
        "",
        "0",
        "false",
        "no",
    )


def _install() -> None:
    if _sqlglot_c_disabled() and not any(
        isinstance(finder, _PurePythonSqlglotFinder) for finder in sys.meta_path
    ):
        sys.meta_path.insert(0, _PurePythonSqlglotFinder())
        logger.warning(
            "DATAHUB_SQLGLOT_DISABLE_C is set; forcing pure-Python sqlglot and "
            "skipping the native C (mypyc) extensions."
        )


_install()
