"""
Venv utilities - static functions for venv paths, markers and configuration queries.

Everything here answers a question about a venv without touching one. Anything
with a side effect belongs in venv_cache.
"""

import logging
import pathlib

from datahub.executor.common.env_config import get_venv_cache_path

logger = logging.getLogger(__name__)

# Version constants
VENV_VERSION_LATEST = "latest"
VENV_VERSION_BUNDLED = "bundled"
VENV_VERSION_NATIVE = "native"

# Every directory the venv cache owns is named with this prefix. Eviction
# filters on it, so it must not drift from venv_location -- the cache root can
# be pointed at a shared directory via DATAHUB_VENV_CACHE_PATH, and anything
# there without this prefix belongs to somebody else.
ENTRY_PREFIX = "venv-"


def is_bundled_version(version: str) -> bool:
    """Check if the version is a bundled version."""
    return version == VENV_VERSION_BUNDLED


def should_use_bundled_venv(version: str) -> bool:
    """Determine if bundled venv should be used based on version."""
    return is_bundled_version(version)


def get_venv_path(venv_name: str, tmp_dir: str) -> str:
    """Get venv path based on venv name and temporary directory."""
    if venv_name.endswith("-bundled"):
        return f"/opt/datahub/venvs/{venv_name}"
    return f"{tmp_dir}/{ENTRY_PREFIX}{venv_name}"


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
        return f"{get_venv_cache_path(tmp_dir)}/{ENTRY_PREFIX}{venv_name}"
    return f"{tmp_dir}/{ENTRY_PREFIX}{venv_name}"


# Written as the LAST step of a successful build, and required before reuse.
# Rename-after-build would be the usual way to make a build atomic, and it is
# unavailable here: venv console scripts hard-code the absolute venv path in
# their shebang (#!/.../venv/bin/python3), and validate_venv requires
# bin/datahub, so a venv built at one path is broken at another.
COMPLETE_MARKER = ".datahub-venv-complete"

# The LRU signal. Deliberately not filesystem atime: container filesystems are
# routinely mounted relatime or noatime, so atime either lags by a day or never
# updates -- an eviction policy built on it looks correct in development and
# does the wrong thing in production.
LAST_USED_MARKER = ".datahub-venv-last-used"


def is_venv_complete(venv_loc: pathlib.Path) -> bool:
    """Whether this venv finished installing and may be reused.

    Both conditions: the interpreter AND the marker. bin/python alone is what a
    killed build leaves behind, and the marker alone is what survives a system
    that clears files out of temp directories but keeps the directories.
    """
    return (venv_loc / "bin" / "python").exists() and (
        venv_loc / COMPLETE_MARKER
    ).exists()


def mark_venv_complete(venv_loc: pathlib.Path) -> None:
    (venv_loc / COMPLETE_MARKER).touch()


def touch_last_used(venv_loc: pathlib.Path) -> None:
    """Record a cache hit. Never allowed to fail a run: a read-only or full
    filesystem costs us eviction accuracy, not the task."""
    try:
        (venv_loc / LAST_USED_MARKER).touch()
    except OSError:
        logger.debug("Could not touch the last-used marker in %s", venv_loc)


def last_used_at(venv_loc: pathlib.Path) -> float:
    """Epoch seconds of the last recorded hit, or 0.0 when never recorded.

    0.0 rather than "now" so an entry predating this feature sorts oldest and
    is evicted first, instead of being immortal.
    """
    try:
        return (venv_loc / LAST_USED_MARKER).stat().st_mtime
    except OSError:
        return 0.0
