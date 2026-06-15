"""Tests for OAuth2 config helpers and init --oauth CLI path."""

import base64
import json
import time
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from datahub.cli.config_utils import (
    OAuthSessionConfig,
    refresh_oauth_token_if_needed,
    write_oauth_config,
)
from datahub.entrypoints import init

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / ".datahubenv"
    monkeypatch.setattr("datahub.entrypoints.DATAHUB_CONFIG_PATH", str(path))
    monkeypatch.setattr("datahub.cli.config_utils.DATAHUB_CONFIG_PATH", str(path))
    return path


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in [
        "DATAHUB_GMS_URL",
        "DATAHUB_GMS_TOKEN",
        "DATAHUB_USERNAME",
        "DATAHUB_PASSWORD",
    ]:
        monkeypatch.delenv(var, raising=False)


def _make_jwt(exp_seconds_from_now: float, include_exp: bool = True) -> str:
    """Build a minimal unsigned JWT with an exp claim for testing."""
    header = (
        base64.urlsafe_b64encode(json.dumps({"alg": "HS256"}).encode())
        .rstrip(b"=")
        .decode()
    )
    payload_data: dict = {"sub": "test-user"}
    if include_exp:
        payload_data["exp"] = int(time.time() + exp_seconds_from_now)
    payload = (
        base64.urlsafe_b64encode(json.dumps(payload_data).encode())
        .rstrip(b"=")
        .decode()
    )
    return f"{header}.{payload}.fakesig"


# ---------------------------------------------------------------------------
# write_oauth_config
# ---------------------------------------------------------------------------


class TestWriteOAuthConfig:
    def test_writes_gms_and_oauth_sections(self, config_path: Path) -> None:
        write_oauth_config(
            host="https://example.datahub.io/gms",
            access_token="at-123",
            client_id="dcr-client-1",
            refresh_token="rt-456",
        )

        data = yaml.safe_load(config_path.read_text())
        assert data["gms"]["server"] == "https://example.datahub.io/gms"
        assert data["gms"]["token"] == "at-123"
        assert data["oauth"]["client_id"] == "dcr-client-1"
        assert data["oauth"]["refresh_token"] == "rt-456"
        assert "token_expiry" not in data["oauth"]

    def test_omits_refresh_token_when_none(self, config_path: Path) -> None:
        write_oauth_config(
            host="https://example.datahub.io/gms",
            access_token="at-123",
            client_id="dcr-client-1",
            refresh_token=None,
        )
        data = yaml.safe_load(config_path.read_text())
        assert "refresh_token" not in data["oauth"]
        assert "token_expiry" not in data["oauth"]

    def test_stores_token_endpoint_when_provided(self, config_path: Path) -> None:
        write_oauth_config(
            host="https://example.datahub.io/gms",
            access_token="at-123",
            client_id="dcr-client-1",
            refresh_token="rt-456",
            token_endpoint="https://example.datahub.io/auth/oauth2/token",
        )
        data = yaml.safe_load(config_path.read_text())
        assert (
            data["oauth"]["token_endpoint"]
            == "https://example.datahub.io/auth/oauth2/token"
        )

    def test_omits_token_endpoint_when_not_provided(self, config_path: Path) -> None:
        write_oauth_config(
            host="https://example.datahub.io/gms",
            access_token="at-123",
            client_id="dcr-client-1",
            refresh_token="rt-456",
        )
        data = yaml.safe_load(config_path.read_text())
        assert "token_endpoint" not in data["oauth"]


# ---------------------------------------------------------------------------
# OAuthSessionConfig
# ---------------------------------------------------------------------------


class TestOAuthSessionConfig:
    def test_valid_config(self) -> None:
        cfg = OAuthSessionConfig(client_id="abc", refresh_token="rt")
        assert cfg.client_id == "abc"

    def test_optional_fields_default_to_none(self) -> None:
        cfg = OAuthSessionConfig(client_id="abc")
        assert cfg.refresh_token is None
        assert cfg.token_endpoint is None

    def test_token_endpoint_stored(self) -> None:
        cfg = OAuthSessionConfig(
            client_id="abc",
            token_endpoint="https://example.datahub.io/auth/oauth2/token",
        )
        assert cfg.token_endpoint == "https://example.datahub.io/auth/oauth2/token"

    def test_model_dump_excludes_none_fields(self) -> None:
        cfg = OAuthSessionConfig(
            client_id="abc", refresh_token=None, token_endpoint=None
        )
        dumped = cfg.model_dump(exclude_none=True)
        assert "refresh_token" not in dumped
        assert "token_endpoint" not in dumped


# ---------------------------------------------------------------------------
# refresh_oauth_token_if_needed
# ---------------------------------------------------------------------------


class TestRefreshOAuthTokenIfNeeded:
    def _write_config(
        self,
        config_path: Path,
        token: Optional[str] = None,
        refresh_token: str = "rt-123",
        client_id: str = "dcr-client",
        exp_seconds_from_now: float = 30,
    ) -> None:
        jwt = token or _make_jwt(exp_seconds_from_now)
        data = {
            "gms": {"server": "https://example.datahub.io/gms", "token": jwt},
            "oauth": {"client_id": client_id, "refresh_token": refresh_token},
        }
        with open(config_path, "w") as f:
            yaml.dump(data, f)

    def _mock_token_refresh(
        self, new_token: str = "new-token", new_refresh: str = "new-rt"
    ) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "access_token": new_token,
            "refresh_token": new_refresh,
            "expires_in": 3600,
        }
        return resp

    def test_refreshes_when_near_expiry(self, config_path: Path) -> None:
        self._write_config(config_path, exp_seconds_from_now=30)
        with patch("requests.post", return_value=self._mock_token_refresh("at-new")):
            result = refresh_oauth_token_if_needed()
        assert result == "at-new"

    def test_does_not_refresh_when_plenty_of_time_left(self, config_path: Path) -> None:
        self._write_config(config_path, exp_seconds_from_now=3600)
        with patch("requests.post") as mock_post:
            result = refresh_oauth_token_if_needed()
        assert result is None
        mock_post.assert_not_called()

    def test_updates_config_file_after_refresh(self, config_path: Path) -> None:
        self._write_config(config_path, exp_seconds_from_now=30)
        with patch(
            "requests.post",
            return_value=self._mock_token_refresh("at-refreshed", "rt-refreshed"),
        ):
            refresh_oauth_token_if_needed()

        data = yaml.safe_load(config_path.read_text())
        assert data["gms"]["token"] == "at-refreshed"
        assert data["oauth"]["refresh_token"] == "rt-refreshed"
        assert "token_expiry" not in data["oauth"]

    def test_returns_none_when_no_config_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "datahub.cli.config_utils.DATAHUB_CONFIG_PATH",
            str(tmp_path / "nonexistent.datahubenv"),
        )
        result = refresh_oauth_token_if_needed()
        assert result is None

    def test_returns_none_when_no_oauth_section(self, config_path: Path) -> None:
        data = {"gms": {"server": "https://example.datahub.io/gms", "token": "tok"}}
        with open(config_path, "w") as f:
            yaml.dump(data, f)
        result = refresh_oauth_token_if_needed()
        assert result is None

    def test_returns_none_when_no_refresh_token(self, config_path: Path) -> None:
        data = {
            "gms": {"server": "https://example.datahub.io/gms", "token": "tok"},
            "oauth": {"client_id": "c"},  # no refresh_token
        }
        with open(config_path, "w") as f:
            yaml.dump(data, f)
        result = refresh_oauth_token_if_needed()
        assert result is None

    def test_returns_none_when_token_has_no_exp_claim(self, config_path: Path) -> None:
        self._write_config(config_path, token=_make_jwt(0, include_exp=False))
        with patch("requests.post") as mock_post:
            result = refresh_oauth_token_if_needed()
        assert result is None
        mock_post.assert_not_called()

    def test_non_fatal_on_http_error(self, config_path: Path) -> None:
        self._write_config(config_path, exp_seconds_from_now=30)
        resp = MagicMock()
        resp.status_code = 401
        with patch("requests.post", return_value=resp):
            result = refresh_oauth_token_if_needed()
        assert result is None

    def test_non_fatal_on_network_error(self, config_path: Path) -> None:
        import requests as req

        self._write_config(config_path, exp_seconds_from_now=30)
        with patch("requests.post", side_effect=req.ConnectionError("refused")):
            result = refresh_oauth_token_if_needed()
        assert result is None

    def test_rotates_refresh_token_when_server_issues_new_one(
        self, config_path: Path
    ) -> None:
        self._write_config(config_path, exp_seconds_from_now=30)
        with patch(
            "requests.post",
            return_value=self._mock_token_refresh("at-new", "rt-rotated"),
        ):
            refresh_oauth_token_if_needed()
        data = yaml.safe_load(config_path.read_text())
        assert data["oauth"]["refresh_token"] == "rt-rotated"

    def test_keeps_existing_refresh_token_when_server_does_not_rotate(
        self, config_path: Path
    ) -> None:
        self._write_config(
            config_path, refresh_token="rt-original", exp_seconds_from_now=30
        )
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "access_token": "at-new",
            "expires_in": 3600,
        }  # no new rt
        with patch("requests.post", return_value=resp):
            refresh_oauth_token_if_needed()
        data = yaml.safe_load(config_path.read_text())
        assert data["oauth"]["refresh_token"] == "rt-original"

    def test_uses_stored_token_endpoint_for_refresh(self, config_path: Path) -> None:
        """When token_endpoint is stored in the config, it should be used for refresh."""
        jwt = _make_jwt(30)
        data = {
            "gms": {"server": "https://example.datahub.io/gms", "token": jwt},
            "oauth": {
                "client_id": "dcr-client",
                "refresh_token": "rt-123",
                "token_endpoint": "https://example.datahub.io/auth/oauth2/token",
            },
        }
        with open(config_path, "w") as f:
            yaml.dump(data, f)
        with patch(
            "requests.post", return_value=self._mock_token_refresh("at-new")
        ) as mock_post:
            refresh_oauth_token_if_needed()
        called_url = mock_post.call_args[0][0]
        assert called_url == "https://example.datahub.io/auth/oauth2/token"

    def test_falls_back_to_conventional_endpoint_when_not_stored(
        self, config_path: Path
    ) -> None:
        """When token_endpoint is absent, fall back to the /auth/oauth2/token convention."""
        self._write_config(config_path, exp_seconds_from_now=30)
        with patch(
            "requests.post", return_value=self._mock_token_refresh("at-new")
        ) as mock_post:
            refresh_oauth_token_if_needed()
        called_url = mock_post.call_args[0][0]
        assert called_url == "https://example.datahub.io/gms/auth/oauth2/token"

    def test_cleans_up_legacy_token_expiry_field_on_refresh(
        self, config_path: Path
    ) -> None:
        """Configs written by the old code had a token_expiry field; verify it gets stripped."""
        jwt = _make_jwt(30)
        data = {
            "gms": {"server": "https://example.datahub.io/gms", "token": jwt},
            "oauth": {
                "client_id": "dcr-client",
                "refresh_token": "rt-123",
                "token_expiry": "2026-01-01T00:00:00+00:00",  # legacy field
            },
        }
        with open(config_path, "w") as f:
            yaml.dump(data, f)
        with patch("requests.post", return_value=self._mock_token_refresh("at-new")):
            refresh_oauth_token_if_needed()
        updated = yaml.safe_load(config_path.read_text())
        assert "token_expiry" not in updated["oauth"]


# ---------------------------------------------------------------------------
# datahub init --oauth  (CLI integration)
# ---------------------------------------------------------------------------


class TestInitOAuthFlag:
    @pytest.fixture(autouse=True)
    def _mock_pkce_login(self) -> Any:
        from datahub.cli.oauth_cli import OAuthResult

        result = OAuthResult(
            client_id="dcr-abc",
            access_token="at-from-pkce",
            refresh_token="rt-from-pkce",
            token_endpoint="https://example.datahub.io/auth/oauth2/token",
        )
        with patch("datahub.cli.oauth_cli.pkce_login", return_value=result) as mock:
            self.mock_pkce = mock
            yield mock

    def test_oauth_writes_config_and_succeeds(
        self, config_path: Path, clean_env: None
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            init,
            ["--oauth", "--host", "https://example.datahub.io/gms", "--force"],
        )
        assert result.exit_code == 0, result.output
        assert "Configuration written" in result.output

        data = yaml.safe_load(config_path.read_text())
        assert data["gms"]["token"] == "at-from-pkce"
        assert data["oauth"]["client_id"] == "dcr-abc"
        assert data["oauth"]["refresh_token"] == "rt-from-pkce"
        assert "token_expiry" not in data["oauth"]

    def test_oauth_calls_pkce_login_with_host(
        self, config_path: Path, clean_env: None
    ) -> None:
        runner = CliRunner()
        runner.invoke(
            init,
            ["--oauth", "--host", "https://example.datahub.io/gms", "--force"],
        )
        self.mock_pkce.assert_called_once_with("https://example.datahub.io/gms")

    def test_oauth_prints_refresh_token_stored_message(
        self, config_path: Path, clean_env: None
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            init,
            ["--oauth", "--host", "https://example.datahub.io/gms", "--force"],
        )
        assert "auto-renew" in result.output

    def test_oauth_and_sso_mutually_exclusive(
        self, config_path: Path, clean_env: None
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            init,
            ["--oauth", "--sso", "--host", "https://example.datahub.io/gms"],
        )
        assert result.exit_code != 0
        assert "--oauth and --sso" in result.output

    def test_oauth_and_token_mutually_exclusive(
        self, config_path: Path, clean_env: None
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--oauth",
                "--token",
                "mytoken",
                "--host",
                "https://example.datahub.io/gms",
            ],
        )
        assert result.exit_code != 0
        assert "--oauth cannot be used with --token" in result.output

    def test_oauth_and_username_password_mutually_exclusive(
        self, config_path: Path, clean_env: None
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--oauth",
                "--username",
                "alice",
                "--password",
                "secret",
                "--host",
                "https://example.datahub.io/gms",
            ],
        )
        assert result.exit_code != 0
        assert "--oauth cannot be used with --username" in result.output

    def test_oauth_and_token_duration_mutually_exclusive(
        self, config_path: Path, clean_env: None
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--oauth",
                "--token-duration",
                "ONE_HOUR",
                "--host",
                "https://example.datahub.io/gms",
            ],
        )
        assert result.exit_code != 0
        assert "--token-duration cannot be used with --oauth" in result.output

    def test_oauth_without_refresh_token_omits_auto_renew_message(
        self, config_path: Path, clean_env: None
    ) -> None:
        from datahub.cli.oauth_cli import OAuthResult

        result_no_rt = OAuthResult(
            client_id="dcr-abc",
            access_token="at-only",
            refresh_token=None,
            token_endpoint="https://example.datahub.io/auth/oauth2/token",
        )
        with patch("datahub.cli.oauth_cli.pkce_login", return_value=result_no_rt):
            runner = CliRunner()
            result = runner.invoke(
                init,
                ["--oauth", "--host", "https://example.datahub.io/gms", "--force"],
            )
        assert result.exit_code == 0
        assert "auto-renew" not in result.output
