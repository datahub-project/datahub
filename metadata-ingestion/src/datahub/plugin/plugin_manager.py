"""Plugin manager — install, uninstall, list, and discover plugins.

Plugins are installed directly into the current Python environment via
``uv pip install`` (preferred) or ``pip install`` (fallback).
Discovery uses ``importlib.metadata`` to scan installed packages for
``datahub-plugin.yaml`` manifests — no lockfile is needed.
"""

import functools
import importlib
import importlib.metadata
import importlib.util
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Type

import yaml

if sys.version_info < (3, 10):
    from importlib_metadata import distributions
else:
    from importlib.metadata import distributions

from datahub.plugin.github_resolver import resolve_plugin_spec
from datahub.plugin.plugin_config import (
    _PLUGIN_ID_RE,
    MANIFEST_FILENAME,
    DiscoveredPlugin,
    PluginManifest,
)
from datahub.plugin.registry_client import PluginIndexEntry, RegistryClient

logger = logging.getLogger(__name__)

# Exceptions that indicate a broken or invalid plugin manifest.
# pydantic.ValidationError does NOT inherit from ValueError in Pydantic v2,
# so it must be listed explicitly to prevent one bad manifest from crashing
# all plugin discovery.
_MANIFEST_LOAD_ERRORS: Tuple[Type[BaseException], ...] = (
    yaml.YAMLError,
    ValueError,
    TypeError,
    AttributeError,
    OSError,
    UnicodeDecodeError,
)
try:
    from pydantic import ValidationError as _PydanticValidationError

    _MANIFEST_LOAD_ERRORS = (*_MANIFEST_LOAD_ERRORS, _PydanticValidationError)
except ImportError:
    pass


@functools.lru_cache(maxsize=1)
def _has_uv() -> bool:
    """Return True if ``uv`` is available in the current Python environment."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "uv", "--version"],
            capture_output=True,
            timeout=10,
        )
        return proc.returncode == 0
    except subprocess.TimeoutExpired:
        logger.debug("uv version check timed out; falling back to pip")
        _has_uv.cache_clear()
        return False
    except (FileNotFoundError, OSError):
        logger.debug("uv not available; using pip", exc_info=True)
        return False


def _pip_cmd() -> List[str]:
    """Return the base command for pip operations, preferring uv."""
    if _has_uv():
        return [sys.executable, "-m", "uv", "pip"]
    return [sys.executable, "-m", "pip"]


def _run_pip(
    args: List[str], *, timeout: int = 300
) -> subprocess.CompletedProcess[str]:
    """Run a pip/uv command and return the result.

    Raises RuntimeError on non-zero exit or timeout.
    """
    cmd = [*_pip_cmd(), *args]
    logger.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(
            f"pip command timed out after {timeout} seconds: {' '.join(args)}"
        ) from e
    if result.returncode != 0:
        raise RuntimeError(
            f"pip command failed (exit {result.returncode}):\n{result.stderr}"
        )
    return result


def _find_manifest_path_in_dist(dist: importlib.metadata.Distribution) -> Optional[str]:
    """Locate the datahub-plugin.yaml for a distribution.

    Tries two strategies:
    1. ``dist.files`` — works for regular (non-editable) installs.
    2. ``importlib.util.find_spec`` — finds the package on disk via
       ``top_level.txt``, which works for editable installs where
       ``dist.files`` only contains the ``.pth`` redirect.
    """
    # Strategy 1: scan dist.files (normal installs)
    if dist.files:
        for f in dist.files:
            if f.name == MANIFEST_FILENAME:
                path = str(f.locate())
                if os.path.isfile(path):
                    return path
                break

    # Strategy 2: locate the package directory via importlib (editable installs)
    top_level = dist.read_text("top_level.txt")
    if top_level:
        for pkg_name in top_level.strip().splitlines():
            pkg_name = pkg_name.strip()
            if not pkg_name:
                continue
            try:
                spec = importlib.util.find_spec(pkg_name)
            except (ValueError, ImportError, AttributeError):
                logger.debug(
                    "find_spec failed for package '%s' in dist '%s'; skipping",
                    pkg_name,
                    dist.name,
                    exc_info=True,
                )
                continue
            if spec and spec.origin:
                pkg_dir = os.path.dirname(spec.origin)
                candidate = os.path.join(pkg_dir, MANIFEST_FILENAME)
                if os.path.isfile(candidate):
                    return candidate

    return None


def _load_plugin_from_dist(
    dist: importlib.metadata.Distribution,
) -> Optional[DiscoveredPlugin]:
    """Try to load a DiscoveredPlugin from a distribution, or return None."""
    manifest_path = _find_manifest_path_in_dist(dist)
    if manifest_path is None:
        return None

    with open(manifest_path) as mf:
        data = yaml.safe_load(mf)
    manifest = PluginManifest.model_validate(data)
    package_name = dist.name
    if not package_name:
        logger.warning(
            "Distribution with manifest at %s has no package name; skipping",
            manifest_path,
        )
        return None
    return DiscoveredPlugin(
        manifest=manifest,
        package_name=package_name,
        version=dist.version or "0.0.0",
        location=os.path.dirname(manifest_path),
    )


def discover_plugins() -> Dict[str, DiscoveredPlugin]:
    """Scan installed packages for datahub-plugin.yaml manifests.

    Returns a dict keyed by plugin ID (from the manifest).
    """
    importlib.invalidate_caches()

    result: Dict[str, DiscoveredPlugin] = {}
    for dist in distributions():
        try:
            plugin = _load_plugin_from_dist(dist)
            if plugin is not None:
                result[plugin.manifest.id] = plugin
        except _MANIFEST_LOAD_ERRORS:
            logger.warning(
                "Skipping invalid manifest in %s",
                dist.name,
                exc_info=True,
            )
    return result


def _normalize_pkg(name: str) -> str:
    """Normalize a package name for comparison (PEP 503-ish)."""
    return name.lower().replace("-", "_")


def _find_plugin_in_package(package_name: str) -> Optional[DiscoveredPlugin]:
    """Find a datahub plugin whose installed package matches *package_name*."""
    normalized = _normalize_pkg(package_name)
    for plugin in discover_plugins().values():
        if _normalize_pkg(plugin.package_name) == normalized:
            return plugin
    return None


# Splits a pip requirement / wheel filename down to its package name, e.g.
# "my-plugin==1.0" -> "my-plugin", "my_plugin-1.0-py3-none-any.whl" -> "my_plugin".
_PKG_NAME_SPLIT_RE = re.compile(r"[=<>!~\[; ]")


def _package_name_from_target(pip_target: str) -> Optional[str]:
    """Best-effort package name from a resolved pip target, for reinstall lookup.

    Reliable for wheels and pip specs; returns ``None`` for git URLs (whose
    package name isn't known until pip resolves them).
    """
    if pip_target.endswith(".whl"):
        # Wheel filename convention: {name}-{version}-{python}-{abi}-{platform}.whl
        return os.path.basename(pip_target).split("-")[0] or None
    if pip_target.startswith(("git+", "http://", "https://")):
        return None
    name = _PKG_NAME_SPLIT_RE.split(pip_target, maxsplit=1)[0].strip()
    return name or None


@dataclass(frozen=True)
class InstallTarget:
    """A resolved install request, ready to hand to ``PluginManager.install``."""

    spec: str
    version: Optional[str]
    expected_sha256: Optional[str]
    entry: Optional[PluginIndexEntry]  # the matched registry entry, if any


class PluginManager:
    # ------------------------------------------------------------------
    # Install
    # ------------------------------------------------------------------

    def resolve_install_target(
        self, spec: str, version: Optional[str] = None
    ) -> InstallTarget:
        """Resolve a user-supplied install spec, consulting the marketplace index.

        When *spec* is a bare plugin id (e.g. ``salesforce-source``) listed in a
        configured registry, it is resolved to that plugin's
        ``github:owner/repo`` at the indexed version, and the index's sha256 is
        carried through so the downloaded wheel is verified before install.

        Every other spec form (``github:``, ``pypi:``, a wheel path, or a pinned
        pip requirement) is returned unchanged with no registry lookup — so the
        lookup (and its network call) only happens for a plain id.
        """
        # Only a bare identifier — matching the manifest-id rule — is eligible
        # for a marketplace lookup; anything with a scheme, path, or version
        # operator is left for resolve_plugin_spec to handle directly.
        if not _PLUGIN_ID_RE.match(spec):
            return InstallTarget(
                spec=spec, version=version, expected_sha256=None, entry=None
            )

        entry = RegistryClient().resolve(spec)
        if entry is None:
            # Not in any registry — fall back to treating it as a pip requirement.
            return InstallTarget(
                spec=spec, version=version, expected_sha256=None, entry=None
            )

        if version and version != entry.version:
            # A requested version differs from the indexed one; the index
            # checksum is for the indexed wheel, so it must not be applied to a
            # different version's wheel.
            return InstallTarget(
                spec=f"github:{entry.repo}",
                version=version,
                expected_sha256=None,
                entry=entry,
            )
        return InstallTarget(
            spec=f"github:{entry.repo}",
            version=entry.version,
            expected_sha256=entry.sha256,
            entry=entry,
        )

    def install(
        self,
        spec: str,
        version: Optional[str] = None,
        *,
        expected_sha256: Optional[str] = None,
    ) -> DiscoveredPlugin:
        """Install a plugin from a spec string.

        Supported spec formats:
        - ``github:owner/repo[@version]``
        - Local wheel path (``/path/to/plugin-1.0-py3-none-any.whl``)
        - pip install spec (``my-datahub-plugin==1.0``)

        Installs directly into the current Python environment via pip.
        When *expected_sha256* is supplied (e.g. from a registry index entry),
        a downloaded wheel is verified against it before installation.
        """
        pip_target = resolve_plugin_spec(spec, version, expected_sha256=expected_sha256)

        # Identify what got installed by diffing plugin discovery around the
        # install. Dependencies never carry a datahub-plugin.yaml, so they don't
        # appear here — sidestepping the fragility of parsing pip/uv stdout.
        before = set(discover_plugins())
        _run_pip(["install", pip_target], timeout=300)
        after = discover_plugins()
        new_ids = set(after) - before

        plugin: Optional[DiscoveredPlugin]
        if len(new_ids) == 1:
            plugin = after[next(iter(new_ids))]
        else:
            # Zero new ids => reinstall of an already-present plugin; several =>
            # a bundle providing multiple plugins. In both cases fall back to the
            # package name derived from the resolved target.
            package_name = _package_name_from_target(pip_target)
            plugin = _find_plugin_in_package(package_name) if package_name else None
            if plugin is None and new_ids:
                plugin = after[sorted(new_ids)[0]]

        if plugin is None:
            raise ValueError(
                f"Installed package does not contain a {MANIFEST_FILENAME}. "
                "Ensure the package includes a datahub-plugin.yaml file."
            )

        logger.info("Installed plugin %s@%s", plugin.manifest.id, plugin.version)
        return plugin

    # ------------------------------------------------------------------
    # Uninstall
    # ------------------------------------------------------------------

    def uninstall(self, plugin_id: str) -> None:
        """Remove a plugin via pip uninstall."""
        plugins = discover_plugins()
        if plugin_id not in plugins:
            raise KeyError(f"Plugin '{plugin_id}' is not installed")

        package_name = plugins[plugin_id].package_name
        _run_pip(["uninstall", "-y", package_name], timeout=120)
        logger.info("Uninstalled plugin %s", plugin_id)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def list_installed(self) -> Dict[str, DiscoveredPlugin]:
        """Return all installed plugins discovered via importlib.metadata."""
        return discover_plugins()

    def get_installed(self, plugin_id: str) -> Optional[DiscoveredPlugin]:
        """Return a single installed plugin or None."""
        return discover_plugins().get(plugin_id)

    def is_installed(self, plugin_id: str) -> bool:
        return plugin_id in discover_plugins()
