"""Markers that tool functions carry into registration.

``@min_version`` declares the oldest DataHub a tool works on; the version filter
that reads it is added with the GMS compatibility work. ``@read_only``,
``@destructive`` and ``@open_world`` become the tool's MCP annotations.

Version schemes:
- Cloud versions use 4 parts: 0.x.y.z (e.g. "0.3.16" or "0.3.16.1")
- OSS versions use 3 parts: x.y.z (e.g. "1.4.0")

``@min_version(cloud="0.3.16", oss="1.4.0")`` means available on Cloud >= 0.3.16
and OSS >= 1.4.0; omitting one side makes the tool unavailable there.
"""

import dataclasses
import re
from typing import Callable

from mcp.types import ToolAnnotations


@dataclasses.dataclass(frozen=True)
class VersionRequirement:
    """Minimum DataHub version for a tool; None means not available there."""

    cloud_min: tuple[int, int, int, int] | None = None
    oss_min: tuple[int, int, int, int] | None = None


def parse_version(version_str: str) -> tuple[int, int, int, int]:
    """Parse "1.4.0", "v1.4.0rc1" or "0.3.16.1" into (major, minor, patch, build)."""
    s = version_str[1:] if version_str.startswith("v") else version_str
    match = re.match(r"(\d+)\.(\d+)\.(\d+)(?:\.(\d+))?(?:rc\d+|-.*)?$", s)
    if not match:
        raise ValueError(f"Invalid version format: {version_str!r}")
    return (
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3)),
        int(match.group(4)) if match.group(4) else 0,
    )


# These decorators set attributes on the function itself instead of wrapping it.
# They are stacked (e.g. @read_only over @min_version), and wrapping would hide the
# attributes that inner decorators set from the registration code that reads them.


def min_version(cloud: str | None = None, oss: str | None = None) -> Callable:
    """Declare the minimum DataHub version a tool requires."""
    req = VersionRequirement(
        cloud_min=parse_version(cloud) if cloud else None,
        oss_min=parse_version(oss) if oss else None,
    )

    def decorator(fn: Callable) -> Callable:
        fn._version_requirement = req  # type: ignore[attr-defined]
        return fn

    return decorator


def get_version_requirement(fn: Callable) -> VersionRequirement | None:
    return getattr(fn, "_version_requirement", None)


def read_only(fn: Callable) -> Callable:
    """Declare that a tool changes nothing (MCP ``readOnlyHint``)."""
    fn._read_only_hint = True  # type: ignore[attr-defined]
    return fn


def destructive(fn: Callable) -> Callable:
    """Declare that a tool may remove or overwrite data (MCP ``destructiveHint``)."""
    fn._destructive_hint = True  # type: ignore[attr-defined]
    return fn


def open_world(fn: Callable) -> Callable:
    """Declare that a tool may reach systems outside DataHub (MCP ``openWorldHint``)."""
    fn._open_world_hint = True  # type: ignore[attr-defined]
    return fn


def tool_annotations(fn: Callable, *, read_only: bool | None = None) -> ToolAnnotations:
    """Build a tool's MCP annotations from its @read_only/@destructive/@open_world markers.

    All three hints are always explicit booleans: clients bucket tools without
    hints as unknown, and some client reviews reject tools that omit any of them.
    The MCP spec ignores destructiveHint on read-only tools, so False there is
    harmless. ``read_only`` overrides the @read_only marker.
    """
    if read_only is None:
        read_only = bool(getattr(fn, "_read_only_hint", False))
    return ToolAnnotations(
        read_only_hint=read_only,
        destructive_hint=not read_only
        and bool(getattr(fn, "_destructive_hint", False)),
        open_world_hint=bool(getattr(fn, "_open_world_hint", False)),
    )
