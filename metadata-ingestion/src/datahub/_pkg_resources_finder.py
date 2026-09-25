"""Provide a minimal ``pkg_resources`` when setuptools>=82 has removed it.

setuptools 82 dropped ``pkg_resources``, but the ``sqlalchemy-redshift`` and
``sqlalchemy-cockroachdb`` dialects still ``import pkg_resources`` at load. This
installs a ``sys.meta_path`` finder that resolves ``import pkg_resources`` to
``datahub.utilities.pkg_resources_shim``.

Temporary: removable once ``sqlalchemy>=2`` unblocks pkg_resources-free dialect
releases (see ``test_sqlalchemy_stays_below_2_until_shim_removed``).

The finder is *appended* to ``sys.meta_path`` so the standard finders get first
crack: a real ``pkg_resources`` always wins structurally, and the shim is a pure
fallback. It *aliases* the shim module rather than re-executing its source, so
``pkg_resources`` and ``datahub.utilities.pkg_resources_shim`` are the same
module object with one ``DistributionNotFound`` class (``isinstance``/``except``
match across both names). Imported from ``datahub/__init__.py`` so it is active
before any datahub code (or a dialect it imports) touches ``pkg_resources``.
"""

from __future__ import annotations

import importlib
import logging
import sys
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from types import ModuleType
from typing import Optional, Sequence

logger = logging.getLogger(__name__)

_SHIM_MODULE = "datahub.utilities.pkg_resources_shim"


class _ShimAliasLoader(Loader):
    """Binds ``pkg_resources`` to the existing shim module object (one identity)."""

    def create_module(self, spec: ModuleSpec) -> ModuleType:
        # Runs only on a real ``import pkg_resources`` (not a find_spec probe),
        # and only because the standard finders found no real pkg_resources.
        logger.warning(
            "pkg_resources is unavailable (setuptools>=82); loading the datahub "
            "compatibility shim in its place."
        )
        return importlib.import_module(_SHIM_MODULE)

    def exec_module(self, module: ModuleType) -> None:
        pass  # import_module already initialized the shim module


class _PkgResourcesShimFinder(MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[str]],
        target: Optional[ModuleType] = None,
    ) -> Optional[ModuleSpec]:
        if fullname != "pkg_resources":
            return None
        return ModuleSpec(fullname, _ShimAliasLoader())


def _install() -> None:
    if not any(isinstance(f, _PkgResourcesShimFinder) for f in sys.meta_path):
        sys.meta_path.append(_PkgResourcesShimFinder())


_install()
