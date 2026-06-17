"""Tests for datahub.cli.oauth_cli — the PKCE authorization code flow."""

import threading
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.cli.oauth_cli import (
    OAuthResult,
    _check_cloud_instance,
    _discover_oauth_server,
    _generate_pkce_pair,
    _make_callback_handler,
    _register_cli_client,
    pkce_login,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DISCOVERY_DOC: Dict[str, Any] = {
    "issuer": "https://example.datahub.io",
    "authorization_endpoint": "https://example.datahub.io/auth/oauth2/authorize",
    "token_endpoint": "https://example.datahub.io/auth/oauth2/token",
    "registration_endpoint": "https://example.datahub.io/auth/oauth2/register",
    "jwks_uri": "https://example.datahub.io/auth/oauth2/jwks",
}


def _mock_response(
    status_code: int, json_body: Any = None, text: str = ""
) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.text = text
    return resp


# ---------------------------------------------------------------------------
# _discover_oauth_server
# ---------------------------------------------------------------------------


class TestDiscoverOAuthServer:
    def test_returns_metadata_on_success(self) -> None:
        with patch("requests.get", return_value=_mock_response(200, _DISCOVERY_DOC)):
            meta = _discover_oauth_server("https://example.datahub.io/gms")
        assert (
            meta["authorization_endpoint"] == _DISCOVERY_DOC["authorization_endpoint"]
        )
        assert meta["token_endpoint"] == _DISCOVERY_DOC["token_endpoint"]

    def test_strips_trailing_slash_from_url(self) -> None:
        with patch(
            "requests.get", return_value=_mock_response(200, _DISCOVERY_DOC)
        ) as mock_get:
            _discover_oauth_server("https://example.datahub.io/gms/")
        called_url = mock_get.call_args[0][0]
        assert (
            called_url
            == "https://example.datahub.io/gms/.well-known/oauth-authorization-server"
        )

    def test_404_raises_with_helpful_message(self) -> None:
        import click

        with patch("requests.get", return_value=_mock_response(404)):
            with pytest.raises(click.ClickException) as exc_info:
                _discover_oauth_server("http://localhost:8080")
        assert "not enabled" in str(exc_info.value.format_message())
        assert "datahub init --sso" in str(exc_info.value.format_message())

    def test_non_200_non_404_raises(self) -> None:
        import click

        with patch("requests.get", return_value=_mock_response(500)):
            with pytest.raises(click.ClickException) as exc_info:
                _discover_oauth_server("https://example.datahub.io/gms")
        assert "500" in str(exc_info.value.format_message())

    def test_network_error_raises(self) -> None:
        import click

        with patch("requests.get", side_effect=requests.ConnectionError("refused")):
            with pytest.raises(click.ClickException) as exc_info:
                _discover_oauth_server("https://example.datahub.io/gms")
        assert "Cannot reach" in str(exc_info.value.format_message())

    def test_invalid_json_raises(self) -> None:
        import click

        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.side_effect = ValueError("not json")
        with patch("requests.get", return_value=resp):
            with pytest.raises(click.ClickException) as exc_info:
                _discover_oauth_server("https://example.datahub.io/gms")
        assert "not valid JSON" in str(exc_info.value.format_message())

    def test_missing_required_fields_raises(self) -> None:
        import click

        incomplete = {"issuer": "https://example.datahub.io"}
        with patch("requests.get", return_value=_mock_response(200, incomplete)):
            with pytest.raises(click.ClickException) as exc_info:
                _discover_oauth_server("https://example.datahub.io/gms")
        assert "missing required fields" in str(exc_info.value.format_message())


# ---------------------------------------------------------------------------
# _check_cloud_instance
# ---------------------------------------------------------------------------


class TestCheckCloudInstance:
    def test_passes_silently_for_cloud_instance(self) -> None:
        body = {"datahub": {"serverEnv": "cloud"}}
        with patch("requests.get", return_value=_mock_response(200, body)):
            _check_cloud_instance("https://example.datahub.io/gms")  # no exception

    def test_raises_for_non_cloud_server_env(self) -> None:
        import click

        body = {"datahub": {"serverEnv": "dev"}}
        with patch("requests.get", return_value=_mock_response(200, body)):
            with pytest.raises(click.ClickException) as exc_info:
                _check_cloud_instance("http://localhost:8080")
        assert "not a Cloud instance" in exc_info.value.format_message()
        assert "serverEnv='dev'" in exc_info.value.format_message()

    def test_passes_when_server_env_missing(self) -> None:
        # /config exists but no datahub.serverEnv — fall through, don't block
        with patch("requests.get", return_value=_mock_response(200, {"other": "data"})):
            _check_cloud_instance("https://example.datahub.io/gms")  # no exception

    def test_passes_on_non_200_response(self) -> None:
        with patch("requests.get", return_value=_mock_response(404)):
            _check_cloud_instance("https://example.datahub.io/gms")  # no exception

    def test_passes_on_network_error(self) -> None:
        with patch("requests.get", side_effect=requests.ConnectionError("refused")):
            _check_cloud_instance("https://example.datahub.io/gms")  # no exception

    def test_passes_on_invalid_json(self) -> None:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.side_effect = ValueError("not json")
        with patch("requests.get", return_value=resp):
            _check_cloud_instance("https://example.datahub.io/gms")  # no exception


# ---------------------------------------------------------------------------
# _register_cli_client
# ---------------------------------------------------------------------------


class TestRegisterCliClient:
    def test_returns_client_id_on_success(self) -> None:
        resp = _mock_response(201, {"client_id": "dcr-abc123"})
        with patch("requests.post", return_value=resp):
            client_id = _register_cli_client(
                "https://example.datahub.io/auth/oauth2/register",
                "http://127.0.0.1:12345/callback",
            )
        assert client_id == "dcr-abc123"

    def test_200_also_accepted(self) -> None:
        resp = _mock_response(200, {"client_id": "dcr-xyz"})
        with patch("requests.post", return_value=resp):
            client_id = _register_cli_client(
                "https://example.datahub.io/auth/oauth2/register",
                "http://127.0.0.1:9999/callback",
            )
        assert client_id == "dcr-xyz"

    def test_403_raises_dcr_not_enabled_message(self) -> None:
        import click

        with patch("requests.post", return_value=_mock_response(403)):
            with pytest.raises(click.ClickException) as exc_info:
                _register_cli_client(
                    "https://example.datahub.io/auth/oauth2/register",
                    "http://127.0.0.1:9999/callback",
                )
        assert "DCR" in str(exc_info.value.format_message())
        assert "datahub init --sso" in str(exc_info.value.format_message())

    def test_401_raises_dcr_not_enabled_message(self) -> None:
        import click

        with patch("requests.post", return_value=_mock_response(401)):
            with pytest.raises(click.ClickException):
                _register_cli_client(
                    "https://example.datahub.io/auth/oauth2/register",
                    "http://127.0.0.1:9999/callback",
                )

    def test_unexpected_status_raises(self) -> None:
        import click

        with patch(
            "requests.post", return_value=_mock_response(400, text="bad request")
        ):
            with pytest.raises(click.ClickException) as exc_info:
                _register_cli_client(
                    "https://example.datahub.io/auth/oauth2/register",
                    "http://127.0.0.1:9999/callback",
                )
        assert "400" in str(exc_info.value.format_message())

    def test_network_error_raises(self) -> None:
        import click

        with patch("requests.post", side_effect=requests.ConnectionError("refused")):
            with pytest.raises(click.ClickException) as exc_info:
                _register_cli_client(
                    "https://example.datahub.io/auth/oauth2/register",
                    "http://127.0.0.1:9999/callback",
                )
        assert "Failed to register" in str(exc_info.value.format_message())

    def test_missing_client_id_in_response_raises(self) -> None:
        import click

        with patch(
            "requests.post", return_value=_mock_response(201, {"something": "else"})
        ):
            with pytest.raises(click.ClickException):
                _register_cli_client(
                    "https://example.datahub.io/auth/oauth2/register",
                    "http://127.0.0.1:9999/callback",
                )


# ---------------------------------------------------------------------------
# _generate_pkce_pair
# ---------------------------------------------------------------------------


class TestGeneratePkcePair:
    def test_returns_two_strings(self) -> None:
        verifier, challenge = _generate_pkce_pair()
        assert isinstance(verifier, str)
        assert isinstance(challenge, str)

    def test_challenge_is_base64url_no_padding(self) -> None:
        _, challenge = _generate_pkce_pair()
        assert "=" not in challenge
        assert "+" not in challenge
        assert "/" not in challenge

    def test_verifier_and_challenge_differ(self) -> None:
        verifier, challenge = _generate_pkce_pair()
        assert verifier != challenge

    def test_s256_derivation_is_correct(self) -> None:
        import base64
        import hashlib

        verifier, challenge = _generate_pkce_pair()
        expected = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
            .rstrip(b"=")
            .decode()
        )
        assert challenge == expected

    def test_pairs_are_unique(self) -> None:
        pairs = [_generate_pkce_pair() for _ in range(5)]
        verifiers = [p[0] for p in pairs]
        assert len(set(verifiers)) == 5


# ---------------------------------------------------------------------------
# _make_callback_handler (loopback server)
# ---------------------------------------------------------------------------


class TestCallbackHandler:
    def _make_request(
        self, path: str, expected_state: str = "test-state"
    ) -> Dict[str, Any]:
        """Spin up the loopback server, send a GET to `path`, return captured result."""
        import http.server

        result: Dict[str, Any] = {}
        handler = _make_callback_handler(result, expected_state)
        server = http.server.HTTPServer(("127.0.0.1", 0), handler)
        port = server.server_address[1]
        t = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True
        )
        t.start()

        import urllib.request

        urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5)
        t.join(timeout=3)
        return result

    def test_captures_authorization_code(self) -> None:
        result = self._make_request("/callback?code=abc123&state=test-state")
        assert result["code"] == "abc123"
        assert result["state"] == "test-state"
        assert result["error"] is None

    def test_captures_error_on_denial(self) -> None:
        result = self._make_request(
            "/callback?error=access_denied&error_description=user+denied"
        )
        assert result["code"] is None
        assert result["error"] == "access_denied"
        assert result["error_description"] == "user denied"


# ---------------------------------------------------------------------------
# pkce_login (integration-style, all network mocked)
# ---------------------------------------------------------------------------


class TestPkceLogin:
    def _token_response(
        self,
        access_token: str = "at-xyz",
        refresh_token: str = "rt-abc",
        expires_in: int = 3600,
    ) -> MagicMock:
        return _mock_response(
            200,
            {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "expires_in": expires_in,
                "token_type": "Bearer",
            },
        )

    @pytest.fixture(autouse=True)
    def _no_browser(self) -> Any:
        with patch("webbrowser.open"):
            yield

    def _run_pkce_login(
        self,
        discovery: Dict[str, Any] = _DISCOVERY_DOC,
        dcr_status: int = 201,
        dcr_body: Any = None,
        token_response: Any = None,
    ) -> OAuthResult:
        """Run pkce_login with the loopback server automatically satisfied."""
        import urllib.request

        dcr_body = dcr_body or {"client_id": "test-client-id"}
        token_response = token_response or self._token_response()

        # Inject the authorization code into the loopback server as soon as it starts
        def _auto_callback(url: str) -> None:
            # Extract the redirect_uri port from the URL and simulate the callback
            from urllib.parse import parse_qs, urlparse

            params = parse_qs(urlparse(url).query)
            redirect_uri = params["redirect_uri"][0]
            callback_url = (
                redirect_uri + "?code=test-code-xyz&state=" + params["state"][0]
            )
            # Small delay so the server thread is ready
            import time

            time.sleep(0.05)
            try:
                urllib.request.urlopen(callback_url, timeout=5)
            except Exception:
                pass

        with (
            patch("requests.get", return_value=_mock_response(200, discovery)),
            patch(
                "requests.post",
                side_effect=[
                    _mock_response(dcr_status, dcr_body),
                    token_response,
                ],
            ),
            patch("webbrowser.open", side_effect=_auto_callback),
        ):
            return pkce_login("https://example.datahub.io/gms", timeout_seconds=10)

    def test_happy_path_returns_oauth_result(self) -> None:
        result = self._run_pkce_login()
        assert result.access_token == "at-xyz"
        assert result.refresh_token == "rt-abc"
        assert result.client_id == "test-client-id"

    def test_no_refresh_token_is_allowed(self) -> None:
        token_resp = _mock_response(
            200, {"access_token": "at-only", "expires_in": 3600}
        )
        result = self._run_pkce_login(token_response=token_resp)
        assert result.access_token == "at-only"
        assert result.refresh_token is None

    def test_oss_instance_raises_clear_message(self) -> None:
        import click

        oss_config = {"datahub": {"serverEnv": "oss"}}
        with patch("requests.get", return_value=_mock_response(200, oss_config)):
            with pytest.raises(click.ClickException) as exc_info:
                pkce_login("http://localhost:8080")
        msg = exc_info.value.format_message()
        assert "not a Cloud instance" in msg
        assert "serverEnv='oss'" in msg

    def test_oauth_server_not_enabled_raises_gracefully(self) -> None:
        import click

        with patch("requests.get", return_value=_mock_response(404)):
            with pytest.raises(click.ClickException) as exc_info:
                pkce_login("http://localhost:8080")
        msg = exc_info.value.format_message()
        assert "not enabled" in msg
        assert "datahub init --sso" in msg

    def test_dcr_not_in_discovery_doc_raises_gracefully(self) -> None:
        import click

        doc_without_dcr = {
            k: v for k, v in _DISCOVERY_DOC.items() if k != "registration_endpoint"
        }
        with patch("requests.get", return_value=_mock_response(200, doc_without_dcr)):
            with pytest.raises(click.ClickException) as exc_info:
                pkce_login("https://example.datahub.io/gms")
        assert "DCR" in exc_info.value.format_message()

    def test_dcr_disabled_403_raises_gracefully(self) -> None:
        import click

        with (
            patch("requests.get", return_value=_mock_response(200, _DISCOVERY_DOC)),
            patch("requests.post", return_value=_mock_response(403)),
        ):
            with pytest.raises(click.ClickException) as exc_info:
                pkce_login("https://example.datahub.io/gms", timeout_seconds=5)
        assert "DCR" in exc_info.value.format_message()

    def test_timeout_raises_gracefully(self) -> None:
        import click

        with (
            patch("requests.get", return_value=_mock_response(200, _DISCOVERY_DOC)),
            patch(
                "requests.post", return_value=_mock_response(201, {"client_id": "c"})
            ),
            patch("webbrowser.open"),  # do NOT simulate callback → timeout
        ):
            with pytest.raises(click.ClickException) as exc_info:
                pkce_login("https://example.datahub.io/gms", timeout_seconds=1)
        assert "timed out" in exc_info.value.format_message()

    def test_state_mismatch_raises_csrf_error(self) -> None:
        """If the callback returns a different state, pkce_login must raise."""
        import urllib.request

        import click

        def _bad_state_callback(url: str) -> None:
            from urllib.parse import parse_qs, urlparse

            params = parse_qs(urlparse(url).query)
            redirect_uri = params["redirect_uri"][0]
            # Inject wrong state — simulates a CSRF attempt
            bad_callback = redirect_uri + "?code=evil-code&state=WRONG"
            import time

            time.sleep(0.05)
            try:
                urllib.request.urlopen(bad_callback, timeout=5)
            except Exception:
                pass

        with (
            patch("requests.get", return_value=_mock_response(200, _DISCOVERY_DOC)),
            patch(
                "requests.post",
                side_effect=[
                    _mock_response(201, {"client_id": "c"}),
                    self._token_response(),
                ],
            ),
            patch("webbrowser.open", side_effect=_bad_state_callback),
        ):
            with pytest.raises(click.ClickException) as exc_info:
                pkce_login("https://example.datahub.io/gms", timeout_seconds=5)
        assert "state mismatch" in exc_info.value.format_message().lower()

    def test_token_endpoint_stored_in_result(self) -> None:
        result = self._run_pkce_login()
        assert result.token_endpoint == _DISCOVERY_DOC["token_endpoint"]


# ---------------------------------------------------------------------------
# _validate_endpoint_origin
# ---------------------------------------------------------------------------


class TestValidateEndpointOrigin:
    def test_same_origin_passes(self) -> None:
        from datahub.cli.oauth_cli import _validate_endpoint_origin

        # Should not raise
        _validate_endpoint_origin(
            "https://example.datahub.io/auth/oauth2/token",
            "https://example.datahub.io/gms",
            "token_endpoint",
        )

    def test_different_netloc_raises(self) -> None:
        import click

        from datahub.cli.oauth_cli import _validate_endpoint_origin

        with pytest.raises(click.ClickException) as exc_info:
            _validate_endpoint_origin(
                "https://attacker.example.com/steal-token",
                "https://example.datahub.io/gms",
                "token_endpoint",
            )
        msg = exc_info.value.format_message()
        assert "different origin" in msg
        assert "attacker.example.com" in msg

    def test_discovery_doc_with_foreign_endpoint_raises(self) -> None:
        import click

        bad_doc = {
            **_DISCOVERY_DOC,
            "token_endpoint": "https://attacker.example.com/token",
        }
        with patch("requests.get", return_value=_mock_response(200, bad_doc)):
            with pytest.raises(click.ClickException) as exc_info:
                _discover_oauth_server("https://example.datahub.io/gms")
        assert "different origin" in exc_info.value.format_message()
