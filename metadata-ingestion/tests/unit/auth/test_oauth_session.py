import base64
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
import yaml

from datahub.cli.config_utils import load_client_config, refresh_oauth_token_if_needed
from datahub.configuration.common import ConfigurationError
from datahub.emitter.token_provider import TokenProviderAuth
from datahub.ingestion.auth import oauth_session
from datahub.ingestion.auth.oauth_session import OAuthSessionTokenProvider
from datahub.ingestion.auth.registry import build_token_provider
from datahub.ingestion.graph.client import get_default_graph


def _jwt(expires_in: float) -> str:
    def encode(value: dict) -> str:
        return (
            base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()
        )

    return f"{encode({'alg': 'HS256'})}.{encode({'exp': int(time.time() + expires_in)})}.sig"


@pytest.fixture
def session_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for name in ("DATAHUB_GMS_URL", "DATAHUB_GMS_TOKEN", "DATAHUB_AUTH_TYPE"):
        monkeypatch.delenv(name, raising=False)
    path = tmp_path / ".datahubenv"
    monkeypatch.setattr("datahub.cli.config_utils.DATAHUB_CONFIG_PATH", str(path))
    return path


def _write_session(path: Path, token: str) -> None:
    path.write_text(
        yaml.dump(
            {
                "gms": {"server": "https://example.datahub.io/gms", "token": token},
                "oauth": {"client_id": "client-1", "refresh_token": "rt-1"},
            }
        )
    )


def _token_response(access_token: str) -> MagicMock:
    response = MagicMock(status_code=200)
    response.json.return_value = {"access_token": access_token, "refresh_token": "rt-2"}
    return response


def test_uses_the_stored_token_while_it_is_valid(session_file: Path) -> None:
    token = _jwt(3600)
    _write_session(session_file, token)

    with patch("requests.post") as post:
        result = OAuthSessionTokenProvider.create(
            {"config_file": str(session_file)}
        ).get_token()

    assert result.token == token
    post.assert_not_called()


def test_clients_sharing_a_session_share_its_refresh(session_file: Path) -> None:
    _write_session(session_file, _jwt(30))
    config = {"config_file": str(session_file)}
    first = OAuthSessionTokenProvider.create(config)
    second = OAuthSessionTokenProvider.create(config)
    new_token = _jwt(3600)

    with patch("requests.post", return_value=_token_response(new_token)) as post:
        assert first.get_token().token == new_token
        assert second.get_token().token == new_token

    # The second client read the first one's refresh from the file instead of
    # spending the rotated refresh token again.
    assert post.call_count == 1
    stored = yaml.safe_load(session_file.read_text())
    assert stored["gms"]["token"] == new_token
    assert stored["oauth"]["refresh_token"] == "rt-2"


def test_a_long_running_client_picks_up_refreshes(session_file: Path) -> None:
    first_token = _jwt(3600)
    _write_session(session_file, first_token)
    session_auth = load_client_config(refresh_per_request=True).auth
    assert session_auth is not None
    auth = TokenProviderAuth(build_token_provider(session_auth))

    def authorization() -> str:
        request = requests.Request("GET", "https://example.datahub.io/gms/config")
        return auth(request.prepare()).headers["Authorization"]

    assert authorization() == f"Bearer {first_token}"
    new_token = _jwt(7200)
    with (
        patch("time.time", return_value=time.time() + 3600 - 60),
        patch("requests.post", return_value=_token_response(new_token)),
    ):
        assert authorization() == f"Bearer {new_token}"


def test_load_client_config_keeps_the_session_token_by_default(
    session_file: Path,
) -> None:
    token = _jwt(3600)
    _write_session(session_file, token)

    assert load_client_config().token == token
    assert load_client_config().auth is None
    per_request = load_client_config(refresh_per_request=True)
    assert per_request.token is None
    assert per_request.auth is not None


def _provider(path: Path) -> OAuthSessionTokenProvider:
    return OAuthSessionTokenProvider.create({"config_file": str(path)})


def test_an_expired_token_that_cannot_be_refreshed_raises(session_file: Path) -> None:
    _write_session(session_file, _jwt(-60))

    with (
        patch("requests.post", side_effect=requests.ConnectionError("refused")),
        pytest.raises(ConfigurationError, match="datahub init --oauth"),
    ):
        _provider(session_file).get_token()


@pytest.mark.parametrize(
    "response",
    [
        MagicMock(status_code=400),
        _token_response(""),
        MagicMock(status_code=200, json=MagicMock(side_effect=ValueError("not json"))),
    ],
)
def test_a_failed_refresh_keeps_the_valid_token(
    session_file: Path, response: MagicMock
) -> None:
    token = _jwt(30)
    _write_session(session_file, token)
    before = session_file.read_text()

    with patch("requests.post", return_value=response):
        assert _provider(session_file).get_token().token == token

    assert session_file.read_text() == before


def test_a_token_without_an_expiry_is_used_as_is(session_file: Path) -> None:
    _write_session(session_file, "opaque-token")

    with patch("requests.post") as post:
        result = _provider(session_file).get_token()

    assert (result.token, result.expires_at) == ("opaque-token", None)
    post.assert_not_called()


def test_a_file_without_a_token_raises(session_file: Path) -> None:
    session_file.write_text(yaml.dump({"gms": {"server": "https://example.io/gms"}}))

    with pytest.raises(ConfigurationError, match="gms.token"):
        _provider(session_file).get_token()


def test_a_failed_write_leaves_the_file_intact(session_file: Path) -> None:
    _write_session(session_file, _jwt(30))
    session_file.chmod(0o600)
    before = session_file.read_text()

    with (
        patch("requests.post", return_value=_token_response(_jwt(3600))),
        patch("yaml.dump", side_effect=OSError("disk full")),
        pytest.raises(OSError),
    ):
        _provider(session_file).get_token()

    assert session_file.read_text() == before
    assert list(session_file.parent.glob(".datahubenv.tmp.*")) == []


def test_a_refresh_keeps_the_file_permissions(session_file: Path) -> None:
    _write_session(session_file, _jwt(30))
    session_file.chmod(0o600)

    with patch("requests.post", return_value=_token_response(_jwt(3600))):
        _provider(session_file).get_token()

    assert session_file.stat().st_mode & 0o777 == 0o600


def test_refreshes_without_a_file_lock_where_fcntl_is_missing(
    session_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(oauth_session, "fcntl", None)
    _write_session(session_file, _jwt(30))
    new_token = _jwt(3600)

    with patch("requests.post", return_value=_token_response(new_token)):
        assert _provider(session_file).get_token().token == new_token


def test_load_client_config_refreshes_an_expiring_session_token(
    session_file: Path,
) -> None:
    _write_session(session_file, _jwt(30))
    new_token = _jwt(3600)

    with patch("requests.post", return_value=_token_response(new_token)):
        assert load_client_config().token == new_token


def test_refresh_oauth_token_if_needed_is_non_fatal(session_file: Path) -> None:
    _write_session(session_file, _jwt(30))

    with patch(
        "datahub.cli.config_utils.read_session_token",
        side_effect=OSError("unreadable"),
    ):
        assert refresh_oauth_token_if_needed() is None


def test_the_default_graph_refreshes_per_request(session_file: Path) -> None:
    _write_session(session_file, _jwt(3600))
    get_default_graph.cache_clear()
    try:
        with (
            patch("datahub.ingestion.graph.client.DataHubGraph.test_connection"),
            patch("datahub.ingestion.graph.client.telemetry_instance"),
        ):
            graph = get_default_graph()
        assert graph.config.token is None
        assert graph.config.auth is not None
        assert graph.config.auth.type == oauth_session.OAUTH_SESSION_AUTH_TYPE
    finally:
        get_default_graph.cache_clear()
