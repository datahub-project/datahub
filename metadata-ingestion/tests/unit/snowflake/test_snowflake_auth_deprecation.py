"""
Unit tests for the Snowflake username+password (DEFAULT_AUTHENTICATOR) auth
deprecation warning.
"""

import warnings

import pytest

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL,
    SnowflakeConnectionConfig,
    get_password_auth_deprecation_warning,
)
from datahub.utilities.global_warning_util import (
    clear_global_warnings,
    get_global_warnings,
)


@pytest.fixture(autouse=True)
def reset_global_warnings():
    clear_global_warnings()
    yield
    clear_global_warnings()


def _password_auth_config_dict() -> dict:
    return {
        "account_id": "acctname",
        "username": "user",  # gitleaks:allow
        "password": "password",  # gitleaks:allow
        "warehouse": "COMPUTE_WH",
        "role": "datahub_role",
    }


def _key_pair_config_dict() -> dict:
    config_dict = _password_auth_config_dict()
    del config_dict["password"]
    config_dict["authentication_type"] = "KEY_PAIR_AUTHENTICATOR"
    config_dict["private_key_path"] = "/a/random/path"
    return config_dict


# ---------------------------------------------------------------------------
# Warning text
# ---------------------------------------------------------------------------


def test_warning_message_includes_migration_guide_link():
    msg = get_password_auth_deprecation_warning()
    assert SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL in msg
    assert "DEFAULT_AUTHENTICATOR" in msg
    assert "KEY_PAIR_AUTHENTICATOR" in msg


# ---------------------------------------------------------------------------
# is_using_password_auth
# ---------------------------------------------------------------------------


def test_is_using_password_auth_flags_default_authenticator_with_password():
    config = SnowflakeConnectionConfig.model_validate(_password_auth_config_dict())
    assert config.is_using_password_auth()


def test_is_using_password_auth_ignores_empty_password():
    # bool(SecretStr("")) is True (the wrapper is truthy), so the predicate must
    # read get_secret_value() to avoid flagging an empty password.
    config_dict = _password_auth_config_dict()
    config_dict["password"] = ""
    config = SnowflakeConnectionConfig.model_validate(config_dict)
    assert not config.is_using_password_auth()


def test_is_using_password_auth_ignores_missing_password():
    config_dict = _password_auth_config_dict()
    del config_dict["password"]
    config = SnowflakeConnectionConfig.model_validate(config_dict)
    assert not config.is_using_password_auth()


def test_is_using_password_auth_ignores_other_auth_modes():
    config = SnowflakeConnectionConfig.model_validate(_key_pair_config_dict())
    assert not config.is_using_password_auth()


# ---------------------------------------------------------------------------
# validate_authentication_config wiring
# ---------------------------------------------------------------------------


def test_config_validation_adds_global_warning_for_password_auth():
    SnowflakeV2Config.model_validate(_password_auth_config_dict())
    deprecation_warnings = [
        w for w in get_global_warnings() if "DEFAULT_AUTHENTICATOR" in w
    ]
    assert len(deprecation_warnings) == 1
    assert SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL in deprecation_warnings[0]


def test_config_validation_emits_configuration_warning_for_password_auth():
    # warnings.warn is the channel --test-source-connection prints.
    with pytest.warns(ConfigurationWarning, match="DEFAULT_AUTHENTICATOR"):
        SnowflakeV2Config.model_validate(_password_auth_config_dict())


def test_config_validation_no_warning_for_key_pair_auth():
    with warnings.catch_warnings():
        warnings.simplefilter("error", ConfigurationWarning)
        SnowflakeV2Config.model_validate(_key_pair_config_dict())
    deprecation_warnings = [
        w for w in get_global_warnings() if "DEFAULT_AUTHENTICATOR" in w
    ]
    assert deprecation_warnings == []
