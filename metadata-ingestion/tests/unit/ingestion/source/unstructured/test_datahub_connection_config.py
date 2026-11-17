"""Unit tests for DataHubConnectionConfig default loading."""

import os
from pathlib import Path
from typing import Iterator, Optional, Tuple
from unittest.mock import patch

import pytest

from datahub.cli import config_utils
from datahub.cli.config_utils import persist_raw_datahub_config, profile_path
from datahub.ingestion.source.unstructured.chunking_config import (
    DataHubConnectionConfig,
)


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.setenv("HOME", str(tmp_path))
    for key in list(os.environ):
        if key.startswith(("DATAHUB_GMS_", "DATAHUB_AUTH_")) or key in {
            "DATAHUB_PROFILE",
            "DATAHUB_SKIP_CONFIG",
        }:
            monkeypatch.delenv(key, raising=False)
    config_utils.reset_profile_cache()
    yield tmp_path
    config_utils.reset_profile_cache()


def _store(name: str, server: str, token: str) -> None:
    persist_raw_datahub_config(
        {"gms": {"server": server, "token": token}}, profile_path(name)
    )


def _pair(config: DataHubConnectionConfig) -> Tuple[str, Optional[str]]:
    return config.server, config.token.get_secret_value() if config.token else None


def test_env_connection_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    _store("default", "http://file:8080", "file-token")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://env:8080")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "env-token")
    assert _pair(DataHubConnectionConfig()) == ("http://env:8080", "env-token")


def test_datahubenv_is_used_without_env() -> None:
    _store("default", "http://file:8080", "file-token")
    assert _pair(DataHubConnectionConfig()) == ("http://file:8080", "file-token")


def test_localhost_when_nothing_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _pair(DataHubConnectionConfig()) == ("http://localhost:8080", None)
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "env-token")
    assert _pair(DataHubConnectionConfig()) == ("http://localhost:8080", "env-token")


def test_localhost_when_config_loading_fails() -> None:
    with patch(
        "datahub.cli.config_utils.load_client_config",
        side_effect=RuntimeError("boom"),
    ):
        assert _pair(DataHubConnectionConfig()) == ("http://localhost:8080", None)


def test_active_profile_is_not_mixed_with_env_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An exported token for another instance must never reach the profile's server.
    _store("selected", "https://selected.example.com/gms", "selected-token")
    config_utils.current_profile_path().write_text("selected\n")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "other-instance-token")
    expected = config_utils.load_client_config()
    assert _pair(DataHubConnectionConfig()) == (expected.server, expected.token)
    assert expected.token == "selected-token"


def test_explicit_values_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://env:8080")
    config = DataHubConnectionConfig(
        server="http://explicit:8080", token="explicit-token"
    )
    assert _pair(config) == ("http://explicit:8080", "explicit-token")
