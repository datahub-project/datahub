"""Helpers shared across phase implementations.

Underscore-prefixed module name signals "internal to ``phases/``" — not meant
to be imported from outside the phases package.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from ..constants import PER_SERVICE_VERSION_KEY, REPO_ROOT, TOKEN_SERVICE_KEYS
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env

log = logging.getLogger(__name__)

_SEED_RECREATE_TIMEOUT_S = 180


def compose_env_for_service(
    service: str,
    image_tag: str,
    passthrough: dict[str, str] | None = None,
    mount_env: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the compose_env dict that pins ``service`` to ``image_tag``.

    Always sets ``DATAHUB_VERSION`` (global fallback), plus the per-service var
    when the service is recognised. Merges ``passthrough`` (token-service
    secrets) so a recreated container doesn't crash on Spring init, and
    ``mount_env`` so host mounts follow the same OLD/NEW side as the image.

    Also forces ``METADATA_SERVICE_AUTH_ENABLED=false`` to preserve the
    auth-bypass SetupOldStackPhase established: Compose's
    ``${METADATA_SERVICE_AUTH_ENABLED:-true}`` substitution reads the parent
    process env, not the env_file, so omitting it silently re-enables auth and
    later phases hit 401 against a token whose MySQL state the nuke wiped.

    Direction-agnostic: ``image_tag`` may be the OLD or the NEW tag.
    """
    env: dict[str, str] = {
        "DATAHUB_VERSION": image_tag,
        "METADATA_SERVICE_AUTH_ENABLED": "false",
    }
    per_service_key = PER_SERVICE_VERSION_KEY.get(service)
    if per_service_key is not None:
        env[per_service_key] = image_tag
    if passthrough:
        env.update(passthrough)
    if mount_env:
        env.update(mount_env)
    return env


@contextmanager
def old_image_gms_window(
    docker: DockerComposeClient,
    *,
    gms_service: str,
    old_image_tag: str,
    new_image_tag: str,
    build_images_root: str,
    log_prefix: str,
) -> Iterator[bool]:
    """Run a block of work with GMS temporarily reverted to the OLD image.

    Yields ``True`` when GMS was actually swapped, ``False`` when there is no
    distinct OLD image to swap to (both tags equal, e.g. under
    ``ZDU_SKIP_BUILD_IMAGES=1``) and the caller should expect NEW-image
    behaviour. GMS is restored on the way out even if the block raises —
    leaving it on OLD would invalidate every later phase.

    Why the sweep scenarios need this. They must observe the batch sweep's own
    cursor and delay mechanics, which requires rows that are still un-migrated
    when the sweep starts. On a deployment that is mid-ZDU-rollout,
    ``featureFlags.aspectMigrationMutatorEnabled`` falls back to
    ``${ZDU_STAGE_20:false}`` and so the write-path
    ``AspectMigrationMutatorChain`` is armed. Two consequences:

    * Seeding through the NEW GMS migrates each aspect to the target version on
      ingest, so the fixture lands already-at-target.
    * The MCE/MAE consumers run *embedded in the GMS process* (``MCE_CONSUMER_ENABLED``
      / ``MAE_CONSUMER_ENABLED``), so a NEW GMS also drains any backlog of
      already-seeded rows through that same armed path — at a few hundred rows a
      second, which is faster than a paced sweep can consume them.

    The second point is why this wraps the *whole* sweep rather than just the
    seed: restoring GMS to NEW straight after seeding hands the fresh fixture to
    an armed consumer, and the rows are gone before the sweep's first batch.

    The OLD image is built without the ZDU test-fixture patch, and that patch is
    what *creates* the mutator classes and ``ZduTestMutatorConfiguration`` — so
    the OLD chain is necessarily empty and neither its write path nor its
    embedded consumers can migrate anything. That also matches how stale rows
    arise in production: written by nodes that predate the upgrade.

    Safe for the sweep itself to run inside the window: the sweep executes in a
    one-shot ``datahub-upgrade`` container with its own ``EntityService`` writing
    directly to MySQL, and the framework polls MySQL directly — neither path
    goes through GMS.
    """
    if old_image_tag == new_image_tag:
        log.warning(
            "%s OLD and NEW image tags are identical (%s) — no distinct OLD "
            "image to run against; proceeding on the running GMS",
            log_prefix,
            old_image_tag,
        )
        yield False
        return

    # Read the token-service secrets off the running GMS before touching it —
    # the recreated container needs them or Spring init rejects the empty
    # signing key.
    token_env = read_token_passthrough(docker, gms_service, purpose="old_image_window")

    def swap(side: str, image_tag: str) -> None:
        # Host mounts have to move with the image: the compose YAML overlays the
        # GMS war and the models resources from a host directory, so leaving them
        # on the NEW worktree while running the OLD image would mix NEW PDL into
        # an OLD container — the mismatch worktree_mount_env exists to prevent.
        mount_env = worktree_mount_env(REPO_ROOT, build_images_root, side)
        log.info(
            "%s recreating %s on %s image (tag=%s, mounts=%s)",
            log_prefix,
            gms_service,
            side.upper(),
            image_tag,
            "pinned" if mount_env else "YAML defaults",
        )
        docker.recreate_service(
            service=gms_service,
            compose_env=compose_env_for_service(
                gms_service, image_tag, passthrough=token_env, mount_env=mount_env
            ),
            timeout_s=_SEED_RECREATE_TIMEOUT_S,
            # Swap the image only. Letting GMS's depends_on cascade fire would
            # re-run system-update inside this window, sweeping the very fixture
            # the window exists to keep un-migrated.
            no_deps=True,
        )

    swap("old", old_image_tag)
    try:
        yield True
    finally:
        swap("new", new_image_tag)


def read_token_passthrough(
    docker: DockerComposeClient,
    gms_service: str,
    *,
    purpose: str,
) -> dict[str, str]:
    """Read JWT signing-key env vars from the running GMS for compose passthrough.

    Five phases (``upgrade_blocking``, ``upgrade_nonblocking``,
    ``rolling_restart``, ``prepare_old_stack``, ``cleanup``) each used to
    repeat this pattern with their own warning string. Centralized so:

    * Any future addition to ``TOKEN_SERVICE_KEYS`` only needs one update.
    * Warning format is consistent — easier to grep across phases.
    * Empty/partial returns are treated the same way everywhere (caller
      decides whether to abort or proceed with degraded behavior).

    ``purpose`` is folded into the warning message so a triager can see
    which call site missed the secrets without grepping for every phase.
    """
    env = docker.get_service_env(gms_service, list(TOKEN_SERVICE_KEYS))
    if not env:
        log.warning(
            "Could not read token-service secrets from %s for %s; "
            "downstream container may fail to start (Spring init will reject "
            "an empty signing key). Missing: %s",
            gms_service,
            purpose,
            list(TOKEN_SERVICE_KEYS),
        )
        return {}
    missing = [k for k in TOKEN_SERVICE_KEYS if k not in env]
    if missing:
        log.warning(
            "Token-service secrets partial for %s: missing %s; container may fail",
            purpose,
            missing,
        )
    return env
