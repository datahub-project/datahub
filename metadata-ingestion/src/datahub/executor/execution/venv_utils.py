"""
Venv utilities - Static functions for venv name generation and configuration queries.

This module provides utilities for determining venv names and paths without
performing any actual venv creation or management.
"""

import hashlib
from typing import Union

from datahub.executor.common.env_config import get_dependency_resolution_enabled

# Version constants
VENV_VERSION_LATEST = "latest"
VENV_VERSION_BUNDLED = "bundled"
VENV_VERSION_NATIVE = "native"


def is_bundled_version(version: str) -> bool:
    """Check if the version is a bundled version."""
    return version == VENV_VERSION_BUNDLED


def should_use_bundled_venv(version: str) -> bool:
    """Whether a run with this requested version uses a pre-built bundled venv.

    A run uses one when it asks for it, or when the executor cannot install packages
    at run time (INGESTION_DEPENDENCY_RESOLUTION_ENABLED=false): the bundled venv is
    then the only environment available, whatever version was requested. "native"
    runs in the executor's own environment and needs no venv either way.
    """
    if version == VENV_VERSION_NATIVE:
        return False
    return is_bundled_version(version) or not get_dependency_resolution_enabled()


def get_venv_name(
    plugin: str,
    version: str,
    extra_pip_requirements: Union[list[str], None] = None,
    extra_pip_plugins: Union[list[str], None] = None,
) -> str:
    """Generate a venv name based on plugin and configuration."""
    if should_use_bundled_venv(version):
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
