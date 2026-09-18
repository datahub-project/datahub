"""
Venv utilities - Static functions for venv name generation and configuration queries.

This module provides utilities for determining venv names and paths without
performing any actual venv creation or management.
"""

import hashlib
from typing import Union

from datahub.executor.common.env_config import get_venv_cache_path

# Version constants
VENV_VERSION_LATEST = "latest"
VENV_VERSION_BUNDLED = "bundled"
VENV_VERSION_NATIVE = "native"


def is_bundled_version(version: str) -> bool:
    """Check if the version is a bundled version."""
    return version == VENV_VERSION_BUNDLED


def should_use_bundled_venv(version: str) -> bool:
    """Determine if bundled venv should be used based on version."""
    return is_bundled_version(version)


def get_venv_name(
    plugin: str,
    version: str,
    extra_pip_requirements: Union[list[str], None] = None,
    extra_pip_plugins: Union[list[str], None] = None,
) -> str:
    """Generate a venv name based on plugin and configuration."""
    if version == VENV_VERSION_BUNDLED:
        return f"{plugin}-bundled"
    # For other versions, use a hash
    suffix = hashlib.sha256()
    suffix.update(version.encode("utf-8"))
    suffix.update(str(extra_pip_requirements or []).encode("utf-8"))
    suffix.update(str(extra_pip_plugins or []).encode("utf-8"))
    return f"{plugin}-{suffix.digest().hex()[:16]}"


def get_venv_path(venv_name: str, tmp_dir: str) -> str:
    """Get venv path based on venv name and temporary directory."""
    if venv_name.endswith("-bundled"):
        return f"/opt/datahub/venvs/{venv_name}"
    return f"{tmp_dir}/venv-{venv_name}"


def should_use_bundled_venv_by_name(venv_name: str) -> bool:
    """Determine if venv should be treated as bundled based on its name."""
    return venv_name.endswith("-bundled")


def venv_location(venv_name: str, tmp_dir: str, *, cacheable: bool) -> str:
    """Where this venv should live.

    `cacheable` comes from VenvConfig.get_stable_venv_name() returning a name
    rather than None -- the caller has already decided whether this venv's
    contents are fully determined by its name. A cacheable venv goes in the
    node-local cache and survives the task; an ephemeral one stays under the
    execution directory and is deleted with it.
    """
    if should_use_bundled_venv_by_name(venv_name):
        return get_venv_path(venv_name, tmp_dir)
    if cacheable:
        return f"{get_venv_cache_path(tmp_dir)}/venv-{venv_name}"
    return f"{tmp_dir}/venv-{venv_name}"
