"""
Venv utilities - Static functions for venv name generation and configuration queries.

This module provides utilities for determining venv names and paths without
performing any actual venv creation or management.
"""

import hashlib
from typing import Union

from packaging.requirements import InvalidRequirement, Requirement

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


def is_moving_requirement(requirement: str) -> bool:
    """Whether this requirement can resolve to a different build over time.

    A venv is cached under a hash of the requirement *strings*, so a requirement whose text is
    stable but whose resolution is not -- ``acryl-datahub-cloud-docs==2.1.*``, ``>=1.2``, or a
    bare package name -- would keep the first venv forever and never see a newer release. The
    caller resolves these to concrete versions before naming the venv; everything else is already
    pinned and needs no lookup.

    Direct references (``pkg @ file:///path``, ``pkg @ https://...``) count as pinned: they name
    exactly one artifact, and re-resolving them would cost a network round trip to learn nothing.
    """
    try:
        parsed = Requirement(requirement)
    except InvalidRequirement:
        # Not something we can reason about (a bare path, an option line). Treat it as pinned
        # rather than resolving it on every run -- the previous behaviour, unchanged.
        return False

    if parsed.url:
        return False

    specs = list(parsed.specifier)
    if len(specs) != 1:
        # No specifier at all, or a compound range. Either way the resolution can move.
        return True
    only = specs[0]
    return only.operator not in ("==", "===") or "*" in only.version


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
