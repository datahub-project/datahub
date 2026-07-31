"""Helpers shared across phase implementations.

Underscore-prefixed module name signals "internal to ``phases/``" — not meant
to be imported from outside the phases package.
"""

from __future__ import annotations

import logging

from ..constants import TOKEN_SERVICE_KEYS
from ..docker_compose import DockerComposeClient

log = logging.getLogger(__name__)


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
