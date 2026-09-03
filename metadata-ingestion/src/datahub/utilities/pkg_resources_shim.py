import importlib
import importlib.metadata as _im
import importlib.resources as _ir
import logging
import os
import sys
import types
from typing import List

from packaging.version import Version, parse as _parse

logger = logging.getLogger(__name__)

_SHIM_NAME = "pkg_resources"

# sqlalchemy-redshift and sqlalchemy-cockroachdb import pkg_resources at module
# load; setuptools>=82 removed it. This provides the minimal API they use.


class DistributionNotFound(Exception):
    """Replacement for pkg_resources.DistributionNotFound."""


def parse_version(version: str) -> Version:
    return _parse(version)


class _Distribution:
    def __init__(self, project_name: str, version: str) -> None:
        self.project_name = project_name
        self.version = version

    @property
    def parsed_version(self) -> Version:
        return _parse(self.version)


def get_distribution(name: str) -> "_Distribution":
    try:
        return _Distribution(name, _im.version(name))
    except _im.PackageNotFoundError as e:
        raise DistributionNotFound(str(e)) from e


def require(requirement: str) -> List["_Distribution"]:
    # Unlike real pkg_resources, this does not resolve or activate dependency sets.
    return [get_distribution(requirement)]


def resource_filename(package_or_requirement: str, resource_name: str) -> str:
    # Reject path traversal (defense-in-depth).
    parts = resource_name.replace("\\", "/").split("/")
    if os.path.isabs(resource_name) or ".." in parts:
        raise ValueError(f"unsafe resource name: {resource_name!r}")

    module = importlib.import_module(package_or_requirement)
    anchor = package_or_requirement
    if getattr(module, "__path__", None) is None:
        # files() needs a package anchor, not a submodule.
        anchor = module.__package__ or package_or_requirement

    ref = _ir.files(anchor)
    for part in parts:
        ref = ref.joinpath(part)
    # Must stay a real path: redshift opens it as sslrootcert at connect time.
    return str(ref)


def declare_namespace(name: str) -> None:
    return None  # PEP 420 namespace packages need no registration.


def fixup_namespace_packages(path: str, *args: object, **kwargs: object) -> None:
    return None  # No-op for PEP 420; called by pytest's syspath_prepend.


def _make_shim() -> types.ModuleType:
    def __getattr__(name: str) -> object:
        raise AttributeError(
            f"pkg_resources shim does not implement {name!r}; it provides only "
            "get_distribution, require, parse_version, resource_filename, "
            "declare_namespace, fixup_namespace_packages, DistributionNotFound"
        )

    shim = types.ModuleType(_SHIM_NAME)
    # Via __dict__: keeps mypy happy and registers __getattr__ as a PEP 562 hook.
    shim.__dict__.update(
        {
            "__datahub_shim__": True,  # detection marker
            "DistributionNotFound": DistributionNotFound,
            "get_distribution": get_distribution,
            "require": require,
            "parse_version": parse_version,
            "resource_filename": resource_filename,
            "declare_namespace": declare_namespace,
            "fixup_namespace_packages": fixup_namespace_packages,
            "__getattr__": __getattr__,
        }
    )
    return shim


def ensure_pkg_resources() -> None:
    """Install the pkg_resources shim if the real module is absent (idempotent).

    Call before importing the sqlalchemy-redshift/cockroachdb dialects.
    """
    if _SHIM_NAME in sys.modules:
        return
    try:
        importlib.import_module(_SHIM_NAME)
        return  # real pkg_resources present -> never shadow it
    except ImportError:
        logger.debug("pkg_resources unavailable; installing datahub shim")
        sys.modules[_SHIM_NAME] = _make_shim()
