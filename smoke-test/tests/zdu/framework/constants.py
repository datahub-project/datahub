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
    """The Compose service names one topology profile brings up.

    ``mae`` / ``mce`` are ``None`` on profiles that run the consumers embedded
    in the GMS process rather than as separate containers.
    """

    gms: str
    upgrade: str
    mae: str | None = None
    mce: str | None = None

    @property
    def in_restart_order(self) -> tuple[str, ...]:
        """Services to roll, GMS first.

        Downstream consumers depend on GMS, so restarting it first lets them
        pick up the new image when they cascade-restart on that dependency.
        """
        return tuple(s for s in (self.gms, self.mae, self.mce) if s is not None)


# Topology profiles the framework knows how to drive.
#
# `debug` collapses MCE + MAE into the GMS process (MAE_CONSUMER_ENABLED is set
# on GMS itself), so there are no consumer containers to name. `debug-consumers`
# is the production-shaped split: GMS runs with both consumers disabled and the
# MCL/MCP write paths live in their own containers. The distinction is not
# cosmetic for ZDU — rollback dual-write runs wherever the MCL write path runs,
# so it is only observable in the MAE container under `debug-consumers`.
#
# Every service key in docker/profiles/docker-compose.gms.yml carries the
# profile as a suffix, which is why GMS and system-update are renamed here too
# and not just the consumers.
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
