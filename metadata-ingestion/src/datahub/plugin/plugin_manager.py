"""Plugin manager — install, uninstall, list, and discover plugins.

Plugins are installed directly into the current Python environment via
``uv pip install`` (preferred) or ``pip install`` (fallback).
Discovery uses ``importlib.metadata`` to scan installed packages for
``datahub-plugin.yaml`` manifests — no lockfile is needed.
"""

import functools
import importlib
import importlib.metadata
import logging
import os
import re
import subprocess
import sys
from typing import Dict, List, Optional

import yaml

if sys.version_info < (3, 10):
    from importlib_metadata import distributions
else:
    from importlib.metadata import distributions

from datahub.plugin.github_resolver import (
    GitHubSpec,
    ResolvedWheel,
    download_wheel,
    resolve_github_spec,
)
from datahub.plugin.plugin_config import (
    MANIFEST_FILENAME,
    DiscoveredPlugin,
    PluginManifest,
)

logger = logging.getLogger(__name__)

# Exceptions that indicate a broken or invalid plugin manifest.
# pydantic.ValidationError does NOT inherit from ValueError in Pydantic v2,
# so it must be listed explicitly to prevent one bad manifest from crashing
# all plugin discovery.
_MANIFEST_LOAD_ERRORS = (
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
        version=dist.metadata.get("Version") or "0.0.0",
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


def _find_plugin_in_package(package_name: str) -> Optional[DiscoveredPlugin]:
    """Find a datahub plugin manifest in a specific installed package."""
    importlib.invalidate_caches()

    normalized = package_name.lower().replace("-", "_")
    for dist in distributions():
        if dist.name and dist.name.lower().replace("-", "_") == normalized:
            try:
                return _load_plugin_from_dist(dist)
            except _MANIFEST_LOAD_ERRORS:
                logger.warning(
                    "Failed to load plugin manifest from %s",
                    package_name,
                    exc_info=True,
                )
                return None
    return None


_PIP_PKG_VERSION_RE = re.compile(r"^(.+)-\d")


def _extract_package_name(pip_output: str) -> Optional[str]:
    """Extract the installed package name from pip/uv install stdout.

    Handles both output formats:
    - pip: ``Successfully installed my-plugin-1.0.0``
    - uv:  `` + my-plugin==1.0.0``
    """
    for line in pip_output.splitlines():
        stripped = line.strip()

        # uv format: " + my-plugin==1.0.0"
        if stripped.startswith("+ ") and "==" in stripped:
            return stripped[2:].split("==")[0]

        # pip format: "Successfully installed my-plugin-1.0.0"
        if stripped.startswith("Successfully installed"):
            parts = stripped.split()
            if len(parts) >= 3:
                m = _PIP_PKG_VERSION_RE.match(parts[2])
                return m.group(1) if m else parts[2]

    return None


class PluginManager:
    # ------------------------------------------------------------------
    # Install
    # ------------------------------------------------------------------

    def install(
        self,
        spec: str,
        version: Optional[str] = None,
    ) -> DiscoveredPlugin:
        """Install a plugin from a spec string.

        Supported spec formats:
        - ``github:owner/repo[@version]``
        - Local wheel path (``/path/to/plugin-1.0-py3-none-any.whl``)
        - pip install spec (``my-datahub-plugin==1.0``)

        Installs directly into the current Python environment via pip.
        """
        pip_target = self._resolve_spec(spec, version)
        result = _run_pip(["install", pip_target], timeout=300)

        # Determine the package name from pip output
        package_name = _extract_package_name(result.stdout)
        if package_name is None:
            package_name = (
                os.path.basename(pip_target).split("-")[0]
                if os.path.isfile(pip_target)
                else spec.replace(":", "_").replace("/", "_")
            )

        # Find the manifest via importlib.metadata
        plugin = _find_plugin_in_package(package_name)
        if plugin is None:
            raise ValueError(
                f"Plugin package does not contain a {MANIFEST_FILENAME}. "
                "Ensure the package includes a datahub-plugin.yaml file."
            )

        logger.info("Installed plugin %s@%s", plugin.manifest.id, plugin.version)
        return plugin

    def _resolve_spec(self, spec: str, version: Optional[str]) -> str:
        """Return the pip install target string."""
        # 1) GitHub spec -- download wheel locally so auth works for private repos
        gh = GitHubSpec.parse(spec)
        if gh is not None:
            github_spec = f"github:{gh.owner}/{gh.repo}@{version}" if version else spec
            resolved = resolve_github_spec(github_spec)
            if isinstance(resolved, ResolvedWheel):
                return download_wheel(resolved)
            return resolved.download_url

        # 2) Local wheel file
        if os.path.isfile(spec) and spec.endswith(".whl"):
            return spec

        # 3) Pip spec (e.g. "my-plugin==1.0")
        if version and "==" not in spec:
            return f"{spec}=={version}"
        return spec

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
