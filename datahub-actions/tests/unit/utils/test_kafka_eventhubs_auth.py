import importlib
import logging
import sys
import types
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

MODULE_UNDER_TEST = "datahub_actions.utils.kafka_eventhubs_auth"
VENDOR_MODULE = "azure.identity"


def ensure_fake_vendor(monkeypatch: Any) -> Any:
    """
    Ensure a fake DefaultAzureCredential is available at import path
    azure.identity for environments where the vendor package is not installed.
    Returns the fake module so tests can monkeypatch its behavior.
    """
    # If already present (package installed), just return the real module
    if VENDOR_MODULE in sys.modules:
        return sys.modules[VENDOR_MODULE]

    # Create a minimal fake module matching the direct import path
    fake_mod: Any = types.ModuleType(VENDOR_MODULE)

    class DefaultAzureCredential:
        def get_token(self, scope: str) -> None:  # will be monkeypatched per test
            raise NotImplementedError

    fake_mod.DefaultAzureCredential = DefaultAzureCredential
    monkeypatch.setitem(sys.modules, VENDOR_MODULE, fake_mod)

    return fake_mod


def import_sut(monkeypatch: Any) -> Any:
    """Import or reload the module under test after ensuring the vendor symbol exists."""
    ensure_fake_vendor(monkeypatch)
    if MODULE_UNDER_TEST in sys.modules:
        return importlib.reload(sys.modules[MODULE_UNDER_TEST])
    return importlib.import_module(MODULE_UNDER_TEST)


def test_oauth_cb_success_returns_token_and_expiry(monkeypatch: Any) -> None:
    """Test that oauth_cb successfully returns an access token and expiry time."""
    sut = import_sut(monkeypatch)

    # Create a fake token object
    fake_token = MagicMock()
    fake_token.token = "my-azure-token"
    fake_token.expires_on = 1234567890.0  # Unix timestamp

    # Monkeypatch the credential to return the fake token
    credential_class = cast(Any, sut).DefaultAzureCredential

    def fake_get_token(self: Any, scope: str) -> Any:
        return fake_token

    monkeypatch.setattr(credential_class, "get_token", fake_get_token)

    token, expiry_time = sut.oauth_cb({"any": "config"})

    assert token == "my-azure-token"
    assert expiry_time == 1234567890.0


def test_oauth_cb_uses_correct_scope(monkeypatch: Any) -> None:
    """Test that oauth_cb requests the correct Azure Event Hubs scope."""
    sut = import_sut(monkeypatch)

    fake_token = MagicMock()
    fake_token.token = "token"
    fake_token.expires_on = 9999999999.0

    captured_scope = []

    credential_class = cast(Any, sut).DefaultAzureCredential

    def fake_get_token(self: Any, scope: str) -> Any:
        captured_scope.append(scope)
        return fake_token

    monkeypatch.setattr(credential_class, "get_token", fake_get_token)

    sut.oauth_cb({})

    assert len(captured_scope) == 1
    assert captured_scope[0] == "https://eventhubs.azure.net/.default"


def test_oauth_cb_raises_and_logs_on_error(monkeypatch: Any, caplog: Any) -> None:
    """Test that oauth_cb properly raises and logs errors."""
    sut = import_sut(monkeypatch)

    def boom(self: Any, scope: str) -> None:
        raise RuntimeError("Azure authentication failed")

    credential_class = cast(Any, sut).DefaultAzureCredential
    monkeypatch.setattr(credential_class, "get_token", boom)

    caplog.set_level(logging.ERROR)

    with pytest.raises(RuntimeError, match="Azure authentication failed"):
        sut.oauth_cb({})

    # Verify the error log is present and descriptive
    assert any(
        rec.levelno == logging.ERROR
        and "Error generating Azure Event Hubs authentication token" in rec.getMessage()
        for rec in caplog.records
    )


def test_oauth_cb_returns_tuple_types(monkeypatch: Any) -> None:
    """Test that oauth_cb returns proper tuple types."""
    sut = import_sut(monkeypatch)

    fake_token = MagicMock()
    fake_token.token = "token123"
    fake_token.expires_on = 1000.5

    credential_class = cast(Any, sut).DefaultAzureCredential
    monkeypatch.setattr(credential_class, "get_token", lambda self, scope: fake_token)

    result = sut.oauth_cb(None)

    assert isinstance(result, tuple)
    token, expiry = result
    assert token == "token123"
    assert isinstance(expiry, float)
    assert expiry == 1000.5


def test_oauth_cb_logs_warning_when_namespace_not_set(
    monkeypatch: Any, caplog: Any
) -> None:
    """Test that a warning is logged when AZURE_EVENT_HUBS_NAMESPACE is not set."""
    sut = import_sut(monkeypatch)

    # Ensure AZURE_EVENT_HUBS_NAMESPACE is not set
    monkeypatch.delenv("AZURE_EVENT_HUBS_NAMESPACE", raising=False)

    fake_token = MagicMock()
    fake_token.token = "token"
    fake_token.expires_on = 9999999999.0

    credential_class = cast(Any, sut).DefaultAzureCredential
    monkeypatch.setattr(credential_class, "get_token", lambda self, scope: fake_token)

    caplog.set_level(logging.WARNING)

    sut.oauth_cb({})

    # Verify the warning log is present
    assert any(
        rec.levelno == logging.WARNING
        and "AZURE_EVENT_HUBS_NAMESPACE not set" in rec.getMessage()
        for rec in caplog.records
    )


def test_oauth_cb_with_namespace_env_var(monkeypatch: Any, caplog: Any) -> None:
    """Test that oauth_cb works when AZURE_EVENT_HUBS_NAMESPACE is set."""
    sut = import_sut(monkeypatch)

    # Set the environment variable
    monkeypatch.setenv(
        "AZURE_EVENT_HUBS_NAMESPACE", "mynamespace.servicebus.windows.net"
    )

    fake_token = MagicMock()
    fake_token.token = "token"
    fake_token.expires_on = 9999999999.0

    credential_class = cast(Any, sut).DefaultAzureCredential
    monkeypatch.setattr(credential_class, "get_token", lambda self, scope: fake_token)

    caplog.set_level(logging.WARNING)

    token, expiry = sut.oauth_cb({})

    # Should not log a warning when namespace is set
    assert not any(
        rec.levelno == logging.WARNING
        and "AZURE_EVENT_HUBS_NAMESPACE not set" in rec.getMessage()
        for rec in caplog.records
    )

    assert token == "token"
    assert expiry == 9999999999.0
