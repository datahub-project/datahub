"""Shared constants for the ZDU framework.

Centralizes values that otherwise drift across phases:

* ``REPO_ROOT`` — absolute path to the DataHub repo root, derived from this
  file's location. The old pattern of computing ``Path(__file__).parents[N]``
  at every call site encoded a depth contract that broke silently when files
  moved. Single source of truth lives here.
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

import pathlib

# framework/constants.py → repo_root four parents up:
# constants.py → framework → zdu → tests → smoke-test → <repo_root>
REPO_ROOT: pathlib.Path = pathlib.Path(__file__).parents[4]

# Compose-debug service names. Order matters for rolling restart: GMS first
# (downstream services depend on it; restarting it first lets MAE/MCE pick up
# the new image when they cascade-restart on dependency).
GMS_SERVICE: str = "datahub-gms-debug"
MAE_SERVICE: str = "datahub-mae-consumer-debug"
MCE_SERVICE: str = "datahub-mce-consumer-debug"

ZDU_SERVICES_IN_ORDER: tuple[str, ...] = (GMS_SERVICE, MAE_SERVICE, MCE_SERVICE)

# Service → per-service version override env var. The compose YAML reads
# ``${DATAHUB_<role>_VERSION:-${DATAHUB_VERSION:-debug}}`` for each, so setting
# the per-service var pins exactly that service to a tag while leaving others
# on the global fallback.
PER_SERVICE_VERSION_KEY: dict[str, str] = {
    GMS_SERVICE: "DATAHUB_GMS_VERSION",
    MAE_SERVICE: "DATAHUB_MAE_VERSION",
    MCE_SERVICE: "DATAHUB_MCE_VERSION",
}

# Token-service JWT signing inputs. Without these, system-update-debug crashes
# Spring init on "signingKey must be set and not be empty". Every phase that
# recreates a service has to forward them through compose_env (Compose's YAML
# substitution layer reads from the parent process env, not from env_files).
TOKEN_SERVICE_KEYS: tuple[str, ...] = (
    "DATAHUB_TOKEN_SERVICE_SIGNING_KEY",
    "DATAHUB_TOKEN_SERVICE_SALT",
)
