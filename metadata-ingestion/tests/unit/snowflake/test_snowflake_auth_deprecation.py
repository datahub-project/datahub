"""
Unit tests for the Snowflake username+password auth deprecation warning.

Covers the detection predicate, the soft/hard-error escalation, and the wiring
into SnowflakeConnectionConfig validation (global warning) — see ING-3479.
"""

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.snowflake.snowflake_auth_deprecation import (
    SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL,
    SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR_ENV,
    check_password_auth_deprecation,
    get_password_auth_deprecation_warning,
    is_hard_error_enabled,
    is_using_password_auth,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.utilities.global_warning_util import (
    clear_global_warnings,
    get_global_warnings,
)


@pytest.fixture(autouse=True)
def reset_global_warnings():
    clear_global_warnings()
    yield
    clear_global_warnings()


@pytest.fixture
def hard_error_enabled(monkeypatch):
    monkeypatch.setenv(SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR_ENV, "true")
    yield
    monkeypatch.delenv(SNOWFLAKE_PASSWORD_AUTH_HARD_ERROR_ENV, raising=False)


class _SecretStrLike:
    """Minimal stand-in for a SecretStr to avoid importing SecretStr here."""

    def __init__(self, value: str) -> None:
        self._value = value

    def __bool__(self) -> bool:
        return bool(self._value)


def test_is_using_password_auth_flags_default_authenticator_with_password():
    assert is_using_password_auth("DEFAULT_AUTHENTICATOR", _SecretStrLike("pw"))


def test_is_using_password_auth_flags_unset_type_with_password():
    # CAT-1921 edge case: UI does not always write authentication_type, so it
    # defaults to DEFAULT_AUTHENTICATOR while a password is still present.
    assert is_using_password_auth("DEFAULT_AUTHENTICATOR", _SecretStrLike("pw"))


def test_is_using_password_auth_ignores_other_auth_modes():
    assert not is_using_password_auth("KEY_PAIR_AUTHENTICATOR", _SecretStrLike("pw"))
    assert not is_using_password_auth("OAUTH_AUTHENTICATOR", _SecretStrLike("pw"))
    assert not is_using_password_auth(
        "EXTERNAL_BROWSER_AUTHENTICATOR", _SecretStrLike("pw")
    )


def test_is_using_password_auth_ignores_missing_password():
    assert not is_using_password_auth("DEFAULT_AUTHENTICATOR", None)
    assert not is_using_password_auth("DEFAULT_AUTHENTICATOR", _SecretStrLike(""))


def test_check_returns_warning_for_password_auth():
    warning = check_password_auth_deprecation(
        "DEFAULT_AUTHENTICATOR", _SecretStrLike("pw")
    )
    assert warning is not None
    assert "DEFAULT_AUTHENTICATOR" in warning
    assert SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL in warning


def test_check_returns_none_for_non_password_auth():
    assert (
        check_password_auth_deprecation("KEY_PAIR_AUTHENTICATOR", _SecretStrLike("pw"))
        is None
    )
    assert check_password_auth_deprecation("DEFAULT_AUTHENTICATOR", None) is None


def test_check_raises_hard_error_when_env_set(hard_error_enabled):
    with pytest.raises(ConfigurationError, match="DEFAULT_AUTHENTICATOR"):
        check_password_auth_deprecation("DEFAULT_AUTHENTICATOR", _SecretStrLike("pw"))


def test_is_hard_error_enabled_defaults_off():
    assert not is_hard_error_enabled()


def test_is_hard_error_enabled_when_env_set(hard_error_enabled):
    assert is_hard_error_enabled()


def test_warning_message_includes_migration_guide_link():
    msg = get_password_auth_deprecation_warning()
    assert SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL in msg
    assert "KEY_PAIR_AUTHENTICATOR" in msg


def _password_auth_config_dict() -> dict:
    return {
        "account_id": "acctname",
        "username": "user",  # noqa: secret  gitleaks:allow
        "password": "password",  # noqa: secret  gitleaks:allow
        "warehouse": "COMPUTE_WH",
        "role": "datahub_role",
    }


def test_config_validation_adds_global_warning_for_password_auth():
    SnowflakeV2Config.model_validate(_password_auth_config_dict())
    warnings = get_global_warnings()
    deprecation_warnings = [w for w in warnings if "DEFAULT_AUTHENTICATOR" in w]
    assert len(deprecation_warnings) == 1
    assert SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL in deprecation_warnings[0]


def test_config_validation_no_warning_for_key_pair_auth():
    config_dict = _password_auth_config_dict()
    del config_dict["password"]
    config_dict["authentication_type"] = "KEY_PAIR_AUTHENTICATOR"
    config_dict["private_key_path"] = "/a/random/path"
    SnowflakeV2Config.model_validate(config_dict)
    deprecation_warnings = [
        w for w in get_global_warnings() if "DEFAULT_AUTHENTICATOR" in w
    ]
    assert deprecation_warnings == []


def test_config_validation_hard_error_when_env_set(hard_error_enabled):
    with pytest.raises(ConfigurationError, match="DEFAULT_AUTHENTICATOR"):
        SnowflakeV2Config.model_validate(_password_auth_config_dict())
