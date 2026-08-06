"""Unit tests for ``phases/_shared.py`` — helper utilities shared across phases."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.constants import TOKEN_SERVICE_KEYS
from tests.zdu.framework.phases._shared import read_token_passthrough


class TestReadTokenPassthrough:
    """R4 / D1 — five phases used to duplicate this pattern. Consolidate the
    expected behavior here.
    """

    def test_returns_env_when_all_keys_present(self) -> None:
        docker = MagicMock()
        docker.get_service_env.return_value = {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey",
            "DATAHUB_TOKEN_SERVICE_SALT": "salt",
        }
        out = read_token_passthrough(docker, "gms-svc", purpose="test-call-site")
        assert out == {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey",
            "DATAHUB_TOKEN_SERVICE_SALT": "salt",
        }
        # Underlying call used the canonical key list.
        docker.get_service_env.assert_called_once_with(
            "gms-svc", list(TOKEN_SERVICE_KEYS)
        )

    def test_returns_empty_dict_when_get_service_env_returns_empty(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        docker = MagicMock()
        docker.get_service_env.return_value = {}
        with caplog.at_level(logging.WARNING):
            out = read_token_passthrough(docker, "gms-svc", purpose="some-phase")
        assert out == {}
        # Warning must surface the call site (``purpose``) so a triager can
        # tell which phase missed the secrets without grepping each one.
        assert any(
            "some-phase" in r.message and "gms-svc" in r.message for r in caplog.records
        )

    def test_warns_on_partial_keys_but_still_returns_what_was_captured(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Only SIGNING_KEY captured; SALT missing.
        docker = MagicMock()
        docker.get_service_env.return_value = {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey",
        }
        with caplog.at_level(logging.WARNING):
            out = read_token_passthrough(docker, "gms-svc", purpose="partial-test")
        # Returns whatever was captured — caller decides whether to abort.
        assert out == {"DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey"}
        # Warning identifies the missing key by name.
        assert any(
            "partial-test" in r.message and "DATAHUB_TOKEN_SERVICE_SALT" in r.message
            for r in caplog.records
        )

    def test_passes_purpose_into_log_message(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Distinct purposes from different phases must each appear in their
        # respective warnings — proves the centralized helper hasn't dropped
        # the call-site context.
        docker = MagicMock()
        docker.get_service_env.return_value = {}
        with caplog.at_level(logging.WARNING):
            read_token_passthrough(docker, "gms-svc", purpose="rolling_restart")
            read_token_passthrough(docker, "gms-svc", purpose="cleanup")
        purposes_logged = {r.message for r in caplog.records}
        assert any("rolling_restart" in m for m in purposes_logged)
        assert any("cleanup" in m for m in purposes_logged)
