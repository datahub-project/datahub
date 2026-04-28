import os
from unittest import mock

import pytest

from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerAPIConfig,
    _DataHubLookerApiSettings,
)

_LOOKERSDK_ENV_VARS = (
    "LOOKERSDK_CLIENT_ID",
    "LOOKERSDK_CLIENT_SECRET",
    "LOOKERSDK_BASE_URL",
)


@pytest.fixture
def clean_lookersdk_env(monkeypatch):
    for var in _LOOKERSDK_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _make_config() -> LookerAPIConfig:
    return LookerAPIConfig(
        base_url="https://looker.example.com",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


def test_credentials_not_written_to_environ(clean_lookersdk_env):
    """LookerAPI must not leak credentials into os.environ."""
    with mock.patch("looker_sdk.init40") as mock_init40:
        mock_init40.return_value = mock.MagicMock()
        LookerAPI(config=_make_config())

    for var in _LOOKERSDK_ENV_VARS:
        assert var not in os.environ, (
            f"{var} should not be set in os.environ after LookerAPI init"
        )


def test_init40_called_with_in_memory_config_settings():
    """init40 must receive a config_settings instance carrying the credentials."""
    config = _make_config()
    with mock.patch("looker_sdk.init40") as mock_init40:
        mock_init40.return_value = mock.MagicMock()
        LookerAPI(config=config)

    mock_init40.assert_called_once()
    _, kwargs = mock_init40.call_args
    settings = kwargs["config_settings"]

    assert isinstance(settings, _DataHubLookerApiSettings)
    assert settings.base_url == config.base_url

    data = settings.read_config()
    assert data["client_id"] == config.client_id
    assert data["client_secret"] == config.client_secret.get_secret_value()
    assert data["base_url"] == config.base_url


def test_settings_read_config_ignores_lookersdk_env_vars(monkeypatch):
    """read_config must return in-memory creds even if LOOKERSDK_* env vars are set."""
    monkeypatch.setenv("LOOKERSDK_CLIENT_ID", "should-be-ignored")
    monkeypatch.setenv("LOOKERSDK_CLIENT_SECRET", "should-be-ignored")
    monkeypatch.setenv("LOOKERSDK_BASE_URL", "https://should-be-ignored.example.com")

    settings = _DataHubLookerApiSettings(
        client_id="real-id",
        client_secret="real-secret",
        base_url="https://real.example.com",
    )

    data = settings.read_config()
    assert data["client_id"] == "real-id"
    assert data["client_secret"] == "real-secret"
    assert data["base_url"] == "https://real.example.com"
    assert settings.base_url == "https://real.example.com"
