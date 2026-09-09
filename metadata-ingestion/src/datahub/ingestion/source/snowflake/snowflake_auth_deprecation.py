"""
Snowflake username+password (``DEFAULT_AUTHENTICATOR``) auth deprecation helpers.

Snowflake is deprecating username + password authentication as part of its
Strong Authentication rollout (Phase 3: Aug-Oct 2026, account-specific
enforcement dates). This module centralises the detection predicate and the
deprecation message used by:

* ``SnowflakeConnectionConfig`` validation -> ``add_global_warning`` (surfaces in
  the CLI ``Global Warnings`` section, ``--strict-warnings``, and telemetry across
  ``datahub ingest run`` / ``datahub check`` / ``--test-source-connection``).
* ``SnowflakeV2Source.__init__`` -> ``self.report.warning`` (surfaces in the
  DataHub UI structured ingestion report).

The warning is **soft** (non-fatal) by default. Set the
``DATAHUB_SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR`` environment variable to a truthy
value to escalate to a hard ``ConfigurationError`` once Snowflake's enforcement
date has passed for a given deployment. This keeps the soft->hard staging
configurable and gated on the Snowflake timeline without a code change.
"""

import os
from typing import Optional

from datahub.configuration.common import ConfigurationError

# Stable URL for the customer-facing migration guide. Linked from the CLI
# warning, the DataHub UI banner, and the structured ingestion report.
SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL = "https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/migrate-to-key-pair-auth"

# Env var that flips the deprecation from a soft warning to a hard error.
# Truthy values: "true", "1", "yes" (case-insensitive). Defaults to unset (soft).
SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR_ENV = "DATAHUB_SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR"

_TRUTHY = {"true", "1", "yes"}


def is_hard_error_enabled() -> bool:
    return (
        os.environ.get(SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR_ENV, "").strip().lower()
        in _TRUTHY
    )


def is_using_password_auth(
    authentication_type: str, password: Optional[object]
) -> bool:
    """
    Return True when the recipe is configured for username+password auth.

    Catches both the explicit case (``authentication_type == DEFAULT_AUTHENTICATOR``
    with a password) and the edge case where ``authentication_type`` is left at its
    default but a password is populated (see CAT-1921: the UI does not always write
    ``authentication_type`` to the YAML, so it defaults to ``DEFAULT_AUTHENTICATOR``).

    ``password`` is typed as ``object`` so callers can pass a ``SecretStr`` without
    importing it here; only its truthiness is inspected, never its value.
    """
    return authentication_type == "DEFAULT_AUTHENTICATOR" and bool(password)


def get_password_auth_deprecation_warning() -> str:
    return (
        "Snowflake is deprecating username + password authentication "
        "(DEFAULT_AUTHENTICATOR) as part of its Strong Authentication rollout. "
        "Password auth will stop working on an account-specific enforcement date "
        "during Phase 3 (Aug-Oct 2026). Switch this recipe to key-pair "
        "authentication (KEY_PAIR_AUTHENTICATOR) before your account's enforcement "
        f"date. See the migration guide: {SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL}"
    )


def check_password_auth_deprecation(
    authentication_type: str, password: Optional[object]
) -> Optional[str]:
    """
    Inspect the auth config and either return the deprecation warning text (soft)
    or raise a ``ConfigurationError`` (hard, when the escalation env var is set).

    Returns ``None`` when the recipe is not using password auth.
    """
    if not is_using_password_auth(authentication_type, password):
        return None

    warning = get_password_auth_deprecation_warning()

    if is_hard_error_enabled():
        raise ConfigurationError(warning)

    return warning
