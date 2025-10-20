from datetime import datetime, timedelta, timezone

from datahub_integrations.slack.config import (
    SlackAppConfigCredentials,
    SlackAppDetails,
    SlackConnection,
)


def test_app_config_tokens_empty_dict_to_none() -> None:
    """Test that empty dict for app_config_tokens is converted to None."""
    # Test empty dict conversion
    config = SlackConnection(app_config_tokens={})  # type: ignore[arg-type]  # Validator converts empty dict to None
    assert config.app_config_tokens is None

    # Test that None stays None
    config = SlackConnection(app_config_tokens=None)
    assert config.app_config_tokens is None

    # Test valid data is preserved
    valid_tokens = SlackAppConfigCredentials(
        access_token="test_access_token",
        refresh_token="test_refresh_token",
        exp=datetime.now(tz=timezone.utc) + timedelta(hours=1),
    )
    config = SlackConnection(app_config_tokens=valid_tokens)
    assert config.app_config_tokens == valid_tokens
    assert config.app_config_tokens.access_token == "test_access_token"


def test_app_details_empty_dict_to_none() -> None:
    """Test that empty dict for app_details is converted to None."""
    # Test empty dict conversion
    config = SlackConnection(app_details={})  # type: ignore[arg-type]  # Validator converts empty dict to None
    assert config.app_details is None

    # Test that None stays None
    config = SlackConnection(app_details=None)
    assert config.app_details is None

    # Test valid data is preserved
    valid_details = SlackAppDetails(
        app_id="test_app_id",
        client_id="test_client_id",
        client_secret="test_client_secret",
    )
    config = SlackConnection(app_details=valid_details)
    assert config.app_details == valid_details
    assert config.app_details.app_id == "test_app_id"


def test_both_validators_work_together() -> None:
    """Test that both validators work correctly when used together."""
    # Both empty dicts
    config = SlackConnection(app_config_tokens={}, app_details={})  # type: ignore[arg-type]  # Validators convert empty dicts to None
    assert config.app_config_tokens is None
    assert config.app_details is None

    # Mix of empty and valid
    valid_details = SlackAppDetails(app_id="test_app")
    config = SlackConnection(app_config_tokens={}, app_details=valid_details)  # type: ignore[arg-type]  # Validators convert empty dicts to None
    assert config.app_config_tokens is None
    assert config.app_details == valid_details

    # Both valid
    valid_tokens = SlackAppConfigCredentials(
        access_token="token", refresh_token="refresh"
    )
    valid_details = SlackAppDetails(app_id="app123")
    config = SlackConnection(app_config_tokens=valid_tokens, app_details=valid_details)
    assert config.app_config_tokens == valid_tokens
    assert config.app_details == valid_details


def test_slack_connection_with_bot_token() -> None:
    """Test SlackConnection with bot_token and empty dict validators."""
    config = SlackConnection(
        app_config_tokens={},  # type: ignore[arg-type]  # Validator converts empty dict to None
        app_details={},  # type: ignore[arg-type]  # Validator converts empty dict to None
        bot_token="xoxb-test-token",
    )
    assert config.app_config_tokens is None
    assert config.app_details is None
    assert config.bot_token == "xoxb-test-token"


def test_slack_app_config_credentials_is_expired() -> None:
    """Test the is_expired method on SlackAppConfigCredentials."""
    # Test expired (default behavior)
    creds = SlackAppConfigCredentials(access_token="token", refresh_token="refresh")
    assert creds.is_expired() is True

    # Test not expired
    future_exp = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    creds = SlackAppConfigCredentials(
        access_token="token", refresh_token="refresh", exp=future_exp
    )
    assert creds.is_expired() is False

    # Test expired with past time
    past_exp = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    creds = SlackAppConfigCredentials(
        access_token="token", refresh_token="refresh", exp=past_exp
    )
    assert creds.is_expired() is True


def test_slack_connection_defaults() -> None:
    """Test that SlackConnection has correct default values."""
    config = SlackConnection()
    assert config.app_config_tokens is None
    assert config.app_details is None
    assert config.bot_token is None
