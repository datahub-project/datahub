"""
Venv utilities - Static functions for venv name generation and configuration queries.

This module provides utilities for determining venv names and paths without
performing any actual venv creation or management.
"""

import hashlib
from enum import Enum, auto
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


class ReqKind(Enum):
    """Three-way classification of requirement lines.

    The install path writes all lines verbatim to a requirements file (option lines are valid
    there). The resolve path needs only MOVING lines but must forward OPTION lines so the
    resolver sees the same indexes and constraints. PINNED lines are skipped entirely — they
    name exactly one artifact and re-resolving them costs a round trip to learn nothing.
    """

    PINNED = auto()
    MOVING = auto()
    OPTION = auto()


def classify_requirement(requirement: str) -> ReqKind:
    """Classify a single requirement line."""
    stripped = requirement.lstrip()
    if stripped.startswith("-"):
        return ReqKind.OPTION

    try:
        parsed = Requirement(requirement)
    except InvalidRequirement:
        return ReqKind.PINNED

    if parsed.url:
        return ReqKind.PINNED

    specs = list(parsed.specifier)
    if len(specs) != 1:
        return ReqKind.MOVING
    only = specs[0]
    if only.operator not in ("==", "===") or "*" in only.version:
        return ReqKind.MOVING
    return ReqKind.PINNED


def partition_requirements(
    reqs: list[str],
) -> dict[ReqKind, list[str]]:
    """Partition a list of requirement lines into PINNED, MOVING, and OPTION.

    Used by both the resolve path (feeds OPTION + MOVING to uv pip compile)
    and the install path (writes all lines verbatim).
    """
    result: dict[ReqKind, list[str]] = {k: [] for k in ReqKind}
    for req in reqs:
        result[classify_requirement(req)].append(req)
    return result


def is_moving_requirement(requirement: str) -> bool:
    """Whether this requirement can resolve to a different build over time.

    Convenience wrapper around classify_requirement for callers that only need the boolean.
    """
    return classify_requirement(requirement) == ReqKind.MOVING


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
