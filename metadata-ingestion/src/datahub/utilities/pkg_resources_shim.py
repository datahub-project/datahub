"""A minimal ``pkg_resources`` replacement, loaded in place of the real module
when setuptools>=82 has removed it.

``stopit``, ``sqlalchemy-redshift`` and ``sqlalchemy-cockroachdb`` all
``import pkg_resources`` at module load. This module implements only the
``pkg_resources`` API those dependencies use; anything else raises via the
module-level ``__getattr__`` below, so a new caller needing more must add it here
after checking the real semantics. It is loaded by the ``sys.meta_path`` finder
in ``datahub/_pkg_resources_finder.py``, which defers to a real ``pkg_resources``
whenever one is installed.
"""

import importlib
import importlib.metadata as _im
import importlib.resources as _ir
import os
from typing import List

from packaging.requirements import InvalidRequirement, Requirement
from packaging.version import Version, parse as _parse

# Detection marker for tests and diagnostics.
__datahub_shim__ = True

_PUBLIC = (
    "DistributionNotFound",
    "get_distribution",
    "require",
    "parse_version",
    "resource_filename",
    "declare_namespace",
    "fixup_namespace_packages",
)


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
    # Accept a bare project name or a PEP 508 requirement string; real
    # pkg_resources parses both. We only read the installed version.
    project = name
    specifier = None
    marker = None
    try:
        req = Requirement(name)
        project = req.name
        specifier = req.specifier
        marker = req.marker
    except InvalidRequirement:
        pass  # not a requirement expression; treat the input as a plain name

    # A requirement whose environment marker excludes this interpreter is not
    # applicable, so there is no distribution to resolve.
    if marker is not None and not marker.evaluate():
        raise DistributionNotFound(f"{name} does not apply to this environment")

    try:
        version = _im.version(project)
    except _im.PackageNotFoundError as e:
        raise DistributionNotFound(str(e)) from e

    if specifier and not specifier.contains(version, prereleases=True):
        raise DistributionNotFound(f"{project} {version} does not satisfy {specifier}")
    return _Distribution(project, version)


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
    path = str(ref)
    # A zip/egg import yields a fabricated (non-filesystem) path; the caller opens
    # this as a real file (redshift's sslrootcert), so fail clearly rather than
    # returning a path that later errors far from here.
    if not os.path.exists(path):
        raise DistributionNotFound(
            f"resource {resource_name!r} in {anchor!r} is not a real filesystem "
            "path (zip/egg imports are not supported by this shim)"
        )
    return path


def declare_namespace(name: str) -> None:
    return None  # PEP 420 namespace packages need no registration.


def fixup_namespace_packages(path: str, *args: object, **kwargs: object) -> None:
    return None  # No-op for PEP 420; called by pytest's syspath_prepend.


def __getattr__(name: str) -> object:
    raise AttributeError(
        f"pkg_resources shim does not implement {name!r}; it provides only: "
        + ", ".join(_PUBLIC)
    )
