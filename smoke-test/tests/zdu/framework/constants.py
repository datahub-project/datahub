"""Shared constants for the ZDU framework.

Centralizes values that otherwise drift across phases:

* ``REPO_ROOT`` — absolute path to the DataHub repo root, derived from this
  file's location. The old pattern of computing ``Path(__file__).parents[N]``
  at every call site encoded a depth contract that broke silently when files
  moved. Single source of truth lives here.
* ``PROFILE_SERVICES`` — Compose service names per topology profile. Every
  service key in ``docker/profiles/docker-compose.gms.yml`` is profile-suffixed,
  so switching profile renames GMS and system-update too, not only the
  consumers. ``ZDUTestConfig.from_env`` derives all four names from here.
* ``ZDU_SERVICES_IN_ORDER`` — the three Compose services the rolling-restart /
  prepare-old-stack phases drive, in dependency order. Was duplicated in 3
  call sites; adding a new service required updating every copy.
* ``PER_SERVICE_VERSION_KEY`` — service-name → per-service version env var.
  Same duplication issue.
* ``TOKEN_SERVICE_KEYS`` — the JWT signing-key env vars that need to flow
  through every stack recreation. Was duplicated in 6 places under varying
  names (``_TOKEN_KEYS``, ``_PRESERVED_KEYS``, ``_REQUIRED_PASSTHROUGH_KEYS``).
"""

from __future__ import annotations

import dataclasses
import pathlib

# framework/constants.py → repo_root four parents up:
# constants.py → framework → zdu → tests → smoke-test → <repo_root>
REPO_ROOT: pathlib.Path = pathlib.Path(__file__).parents[4]


@dataclasses.dataclass(frozen=True)
class ProfileServices:
    """Compose service names one topology profile brings up.

    ``mae`` / ``mce`` are ``None`` where the consumers run inside GMS.
    """

    gms: str
    upgrade: str
    mae: str | None = None
    mce: str | None = None

    @property
    def in_restart_order(self) -> tuple[str, ...]:
        """GMS first — the consumers cascade-restart on that dependency."""
        return tuple(s for s in (self.gms, self.mae, self.mce) if s is not None)


# `debug` runs MAE/MCE inside GMS; `debug-consumers` splits them out, which is
# the production shape and the only one where rollback dual-write runs outside
# GMS. Every service key in docker-compose.gms.yml is profile-suffixed, so the
# profile renames GMS and system-update too, not just the consumers.
PROFILE_SERVICES: dict[str, ProfileServices] = {
    "debug": ProfileServices(
        gms="datahub-gms-debug",
        upgrade="system-update-debug",
    ),
    "debug-consumers": ProfileServices(
        gms="datahub-gms-debug-consumers",
        upgrade="system-update-debug-consumers",
        mae="datahub-mae-consumer-debug-consumers",
        mce="datahub-mce-consumer-debug-consumers",
    ),
}

DEFAULT_PROFILE: str = "debug"

# The incremental-reindex upgrade id is
# "BuildIndicesIncremental_<gitVersion>-<revision>". OLD and NEW are built from
# near-identical commits here, so they share a gitVersion and would share that
# id — the NEW upgrade would inherit the OLD boot's COMPLETED state and skip the
# reindex. Production never collides; its releases differ.
#
# Note the writers (GMS/MAE) are NOT given this revision, so they resolve
# "<gitVersion>-0" and see no Phase-1 state. Aligning them was tried and did
# not make dual-write engage — the reindex deletes the old backing index, so
# there is no dual-write target regardless.
ZDU_NEW_REVISION: str = "1"

GMS_SERVICE: str = PROFILE_SERVICES[DEFAULT_PROFILE].gms
UPGRADE_SERVICE: str = PROFILE_SERVICES[DEFAULT_PROFILE].upgrade

# Consumer containers exist only under `debug-consumers`; the phases that use
# these filter by what the running stack actually reports, so naming the
# `debug-consumers` services here is a safe no-op on `debug`.
MAE_SERVICE: str = PROFILE_SERVICES["debug-consumers"].mae or ""
MCE_SERVICE: str = PROFILE_SERVICES["debug-consumers"].mce or ""

ZDU_SERVICES_IN_ORDER: tuple[str, ...] = (GMS_SERVICE, MAE_SERVICE, MCE_SERVICE)

# Service → per-service version override env var. The compose YAML reads
# ``${DATAHUB_<role>_VERSION:-${DATAHUB_VERSION:-debug}}`` for each, so setting
# the per-service var pins exactly that service to a tag while leaving others
# on the global fallback.
#
# Keyed by every profile's service names: a lookup miss silently drops the
# per-service pin and leaves that container on ${DATAHUB_VERSION}, which in a
# two-image run means the wrong side's image.
PER_SERVICE_VERSION_KEY: dict[str, str] = {
    **{p.gms: "DATAHUB_GMS_VERSION" for p in PROFILE_SERVICES.values()},
    **{p.mae: "DATAHUB_MAE_VERSION" for p in PROFILE_SERVICES.values() if p.mae},
    **{p.mce: "DATAHUB_MCE_VERSION" for p in PROFILE_SERVICES.values() if p.mce},
}

# Token-service JWT signing inputs. Without these, system-update-debug crashes
# Spring init on "signingKey must be set and not be empty". Every phase that
# recreates a service has to forward them through compose_env (Compose's YAML
# substitution layer reads from the parent process env, not from env_files).
TOKEN_SERVICE_KEYS: tuple[str, ...] = (
    "DATAHUB_TOKEN_SERVICE_SIGNING_KEY",
    "DATAHUB_TOKEN_SERVICE_SALT",
)
