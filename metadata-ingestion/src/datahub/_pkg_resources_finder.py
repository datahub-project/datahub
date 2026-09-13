"""Provide a minimal ``pkg_resources`` when setuptools>=82 has removed it.

setuptools 82 dropped ``pkg_resources``, but several ingestion dependencies
(``stopit``, ``sqlalchemy-redshift``, ``sqlalchemy-cockroachdb``) still
``import pkg_resources`` at load. This installs a ``sys.meta_path`` finder that
resolves ``import pkg_resources`` to ``datahub.utilities.pkg_resources_shim``.

The finder is *appended* to ``sys.meta_path`` so the standard finders get first
crack: a real ``pkg_resources`` always wins structurally, and the shim is a pure
fallback that only loads when nothing else provides the module. Imported from
``datahub/__init__.py`` so it is active before any datahub code (or a dialect it
imports) touches ``pkg_resources``.
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

_SHIM_SOURCE = os.path.join(
    os.path.dirname(__file__), "utilities", "pkg_resources_shim.py"
)


class _PkgResourcesShimFinder(MetaPathFinder):
    """Loads the datahub shim as ``pkg_resources`` when the real one is absent."""

    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[str]],
        target: Optional[ModuleType] = None,
    ) -> Optional[ModuleSpec]:
        if fullname != "pkg_resources":
            return None
        # Reached only because the standard finders found no real pkg_resources.
        # Fires once (the module is cached in sys.modules afterward).
        logger.warning(
            "pkg_resources is unavailable (setuptools>=82); loading the datahub "
            "compatibility shim in its place."
        )
        return importlib.util.spec_from_file_location(fullname, _SHIM_SOURCE)


def _install() -> None:
    if not any(isinstance(f, _PkgResourcesShimFinder) for f in sys.meta_path):
        sys.meta_path.append(_PkgResourcesShimFinder())


_install()
