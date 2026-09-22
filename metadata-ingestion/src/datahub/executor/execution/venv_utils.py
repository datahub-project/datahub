"""
Venv utilities - static functions for venv paths, markers and configuration queries.

Everything here answers a question about a venv without touching one. Anything
with a side effect belongs in venv_cache.
"""

import hashlib
import logging
import pathlib
import re

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


# Everything a cache entry's directory name is allowed to contain. Deliberately
# a strict allowlist rather than a blocklist of separators: the input is recipe
# data, and the only characters any real source type uses are already in here.
_UNSAFE_ENTRY_CHARS = re.compile(r"[^A-Za-z0-9._-]")


def _safe_entry_name(venv_name: str) -> str:
    """Reduce a venv name to one path component, without merging distinct names.

    The name carries `main_plugin`, which is recipe["source"]["type"] --
    unvalidated input -- and it is concatenated straight into a node-SHARED
    directory. A separator in it is not cosmetic. `source.type: "mysql/x"`
    makes the entry `<root>/venv-mysql/x-latest-<hash>`, and then:

    - _acquire_cache_entry evicts against `venv_loc.parent`, which is now
      `<root>/venv-mysql` rather than the cache root, so that build never
      trims the real cache; and
    - any OTHER task's eviction pass, running against the real root, sees
      `venv-mysql` as an entry -- it is a directory with the prefix -- finds
      no LAST_USED marker so last_used_at returns 0.0 and it sorts FIRST,
      then takes `<root>/venv-mysql.lock`, a path no live task holds, and
      rmtrees a venv a running ingestion is executing from.

    `..` is the same problem pointed outward: the entry and its lock land
    outside the cache root entirely, where nothing ever reclaims them.

    Substitution alone would be a different bug -- it is many-to-one, so
    `a/b` and `a_b` would share an entry and one recipe would get the other's
    venv. A digest of the ORIGINAL name is appended whenever substitution
    changed anything, which keeps distinct names distinct while leaving every
    already-safe name byte-identical, so existing entries are not orphaned.
    """
    sanitized = _UNSAFE_ENTRY_CHARS.sub("_", venv_name)
    if sanitized == venv_name:
        return venv_name
    logger.warning(
        "venv name %r is not a safe directory name; using %r instead. This "
        "usually means a recipe's source type contains a path separator.",
        venv_name,
        sanitized,
    )
    return f"{sanitized}-{hashlib.sha256(venv_name.encode('utf-8')).hexdigest()[:16]}"


def venv_location(venv_name: str, tmp_dir: str, *, cacheable: bool) -> str:
    """Where this venv should live.

    `cacheable` is decided by the caller (_name_dynamic_venv): a stable name
    AND the cache switched on. A cacheable venv goes in the node-local cache
    and survives the task; an ephemeral one stays under the execution
    directory and is deleted with it.
    """
    venv_name = _safe_entry_name(venv_name)
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


def built_at(venv_loc: pathlib.Path) -> float:
    """Epoch seconds when this venv finished building, or 0.0 if unknown.

    The completion marker is written once, as the last step of a successful
    build, and never touched again -- so its mtime is the build time. This is
    deliberately NOT the last-used marker, which is refreshed on every hit:
    an age measured from last use would make the busiest entry the one that
    never expires, which is exactly backwards for a moving version like
    `latest` that every recipe shares.

    0.0 for an entry with no marker, so an unknown build age reads as
    infinitely old and is rebuilt rather than trusted.
    """
    try:
        return (venv_loc / COMPLETE_MARKER).stat().st_mtime
    except OSError:
        return 0.0


def last_used_at(venv_loc: pathlib.Path) -> float:
    """Epoch seconds of the last recorded hit, or 0.0 when never recorded.

    0.0 rather than "now" so an entry predating this feature sorts oldest and
    is evicted first, instead of being immortal.
    """
    try:
        return (venv_loc / LAST_USED_MARKER).stat().st_mtime
    except OSError:
        return 0.0
