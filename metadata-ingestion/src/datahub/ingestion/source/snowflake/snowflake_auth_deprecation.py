"""
Snowflake username+password (``DEFAULT_AUTHENTICATOR``) auth deprecation helpers.

Snowflake is deprecating username + password authentication as part of its
Strong Authentication rollout (Phase 3: Aug-Oct 2026, account-specific
enforcement dates). The warning is **soft** by default; set
``DATAHUB_SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR`` to escalate to a hard
``ConfigurationError`` once enforcement has passed for a deployment.
"""

from typing import Optional

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.configuration.common import ConfigurationError

# Stable URL — linked from the CLI warning, UI banner, and ingestion report.
SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL = "https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/migrate-to-key-pair-auth"

SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR_ENV = "DATAHUB_SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR"


def is_hard_error_enabled() -> bool:
    return get_boolean_env_variable(SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR_ENV)


def _secret_value(password: Optional[object]) -> Optional[str]:
    """``bool(SecretStr(""))`` is ``True`` (the wrapper is always truthy), so read ``get_secret_value()`` first."""
    if password is None:
        return None
    getter = getattr(password, "get_secret_value", None)
    if callable(getter):
        return getter()
    return str(password)


def is_using_password_auth(
    authentication_type: str, password: Optional[object]
) -> bool:
    """True when the recipe is configured for username+password auth.

    Covers the CAT-1921 edge case where the UI omits ``authentication_type``
    (defaults to ``DEFAULT_AUTHENTICATOR``) but a password is present.
    """
    return authentication_type == "DEFAULT_AUTHENTICATOR" and bool(
        _secret_value(password)
    )


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
    """Return the deprecation warning (soft) or raise ``ConfigurationError`` (hard)."""
    if not is_using_password_auth(authentication_type, password):
        return None

    warning = get_password_auth_deprecation_warning()

    if is_hard_error_enabled():
        raise ConfigurationError(warning)

    return warning
