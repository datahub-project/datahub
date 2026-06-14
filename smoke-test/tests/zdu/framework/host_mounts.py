"""Resolves OLD/NEW worktree host-mount paths for compose env-var indirection.

G20c-Option-A: the dev compose YAML uses host-volume mounts (under
``../../datahub-upgrade/build/libs``, ``../../metadata-models/src/main/resources``,
``../../metadata-service/war/build/libs``) that unconditionally overlay each
container's bundled artifacts. Without indirection, this defeats two-image
testing — even an OLD-tagged image runs with whatever the dev's working tree
last built. The compose YAML now reads those mount sources via
``${DATAHUB_UPGRADE_BIN_HOST_DIR:-...}``-style env vars; phases that need to
pin the mount to a specific worktree (OLD for the initial deploy, NEW for the
upgrade and rolling restart) compute the env-var dict here and pass it through
their ``compose_env`` kwarg to ``DockerComposeClient.up_stack`` /
``run_upgrade_job`` / ``recreate_service``.

Returns ``None`` when the worktree's build artifacts aren't materialized
(e.g. ``ZDU_SKIP_BUILD_IMAGES=1`` and Phase 0 didn't run). The caller treats
``None`` as "let YAML defaults apply" — i.e. keep the dev working tree mounts.
"""

from __future__ import annotations

import logging
import pathlib

log = logging.getLogger(__name__)


# Mapping: env-var name → relative path under the worktree root.
# Keep absolute paths so compose's cwd (docker/profiles/) doesn't change
# resolution. Trailing slash matches the YAML mount syntax.
_WORKTREE_MOUNT_SOURCES: dict[str, str] = {
    "DATAHUB_UPGRADE_BIN_HOST_DIR": "datahub-upgrade/build/libs/",
    "DATAHUB_MODELS_RESOURCES_HOST_DIR": "metadata-models/src/main/resources/",
    "DATAHUB_GMS_WAR_HOST_DIR": "metadata-service/war/build/libs/",
}


def worktree_mount_env(
    repo_root: pathlib.Path,
    build_images_root: str,
    side: str,
) -> dict[str, str] | None:
    """Return env-var dict for compose YAML indirection, or ``None`` if the
    worktree's build outputs aren't present.

    ``side`` must be ``"old"`` or ``"new"`` — names the subdir of
    ``build_images_root`` that ``BuildImagesPhase._worktree_paths`` produces.

    The check looks for each mount source as a directory. If any one is
    missing we fall back to ``None`` (and the YAML defaults stay in effect),
    rather than partially mounting OLD artifacts + dev-tree artifacts — a
    mismatch that would silently corrupt two-image semantics.
    """
    if side not in ("old", "new"):
        raise ValueError(f"side must be 'old' or 'new', got {side!r}")
    worktree = (repo_root / build_images_root / side).resolve()
    if not worktree.is_dir():
        log.info(
            "[host-mounts] %s worktree %s not present — using compose YAML defaults",
            side.upper(),
            worktree,
        )
        return None
    out: dict[str, str] = {}
    for env_var, rel in _WORKTREE_MOUNT_SOURCES.items():
        abs_path = worktree / rel
        if not abs_path.is_dir():
            log.warning(
                "[host-mounts] %s worktree missing %s — falling back to "
                "compose YAML defaults so we don't mix OLD/NEW artifacts",
                side.upper(),
                abs_path,
            )
            return None
        # Compose tolerates either trailing slash or not; keep the slash so
        # the YAML default and the override look identical in logs.
        out[env_var] = f"{abs_path}/"
    return out
