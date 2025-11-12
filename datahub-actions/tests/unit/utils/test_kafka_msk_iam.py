import importlib
import logging
import sys
import types
from typing import Any, cast

import pytest

MODULE_UNDER_TEST = "datahub_actions.utils.kafka_msk_iam"
VENDOR_MODULE = "aws_msk_iam_sasl_signer"


def ensure_fake_vendor(monkeypatch: Any) -> Any:
    """
    Ensure a fake MSKAuthTokenProvider is available at import path
    aws_msk_iam_sasl_signer for environments where the vendor package is not installed.
    Returns the fake module so tests can monkeypatch its behavior.
    """
    # If already present (package installed), just return the real module
    if VENDOR_MODULE in sys.modules:
        return sys.modules[VENDOR_MODULE]

    # Create a minimal fake module matching the direct import path
    fake_mod: Any = types.ModuleType(VENDOR_MODULE)

    class MSKAuthTokenProvider:
        @staticmethod
        def generate_auth_token(
            region: str | None = None,
        ) -> None:  # will be monkeypatched per test
            raise NotImplementedError

    fake_mod.MSKAuthTokenProvider = MSKAuthTokenProvider
    monkeypatch.setitem(sys.modules, VENDOR_MODULE, fake_mod)

    return fake_mod


def import_sut(monkeypatch: Any) -> Any:
    """Import or reload the module under test after ensuring the vendor symbol exists."""
    ensure_fake_vendor(monkeypatch)
    if MODULE_UNDER_TEST in sys.modules:
        return importlib.reload(sys.modules[MODULE_UNDER_TEST])
    return importlib.import_module(MODULE_UNDER_TEST)


def test_oauth_cb_success_converts_ms_to_seconds(monkeypatch: Any) -> None:
    sut = import_sut(monkeypatch)

    # Monkeypatch the provider to return a known token and expiry in ms
    provider = cast(Any, sut).MSKAuthTokenProvider

    def fake_generate(region: str | None = None) -> tuple[str, int]:
        return "my-token", 12_345  # ms

    monkeypatch.setattr(provider, "generate_auth_token", staticmethod(fake_generate))

    token, expiry_seconds = sut.oauth_cb({"any": "config"})

    assert token == "my-token"
    assert expiry_seconds == 12.345  # ms to seconds via division


def test_oauth_cb_raises_and_logs_on_error(monkeypatch: Any, caplog: Any) -> None:
    sut = import_sut(monkeypatch)

    def boom(region: str | None = None) -> None:
        raise RuntimeError("signer blew up")

    provider = cast(Any, sut).MSKAuthTokenProvider
    monkeypatch.setattr(provider, "generate_auth_token", staticmethod(boom))

    caplog.set_level(logging.ERROR)

    with pytest.raises(RuntimeError, match="signer blew up"):
        sut.oauth_cb({})

    # Verify the error log is present and descriptive
    assert any(
        rec.levelno == logging.ERROR
        and "Error generating AWS MSK IAM authentication token" in rec.getMessage()
        for rec in caplog.records
    )


def test_oauth_cb_returns_tuple_types(monkeypatch: Any) -> None:
    sut = import_sut(monkeypatch)

    provider = cast(Any, sut).MSKAuthTokenProvider
    monkeypatch.setattr(
        provider,
        "generate_auth_token",
        staticmethod(lambda region=None: ("tkn", 1_000)),  # 1000 ms
    )

    result = sut.oauth_cb(None)

    assert isinstance(result, tuple)
    token, expiry = result
    assert token == "tkn"
    assert isinstance(expiry, float)
    assert expiry == 1.0
