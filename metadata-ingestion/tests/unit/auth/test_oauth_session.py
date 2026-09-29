import base64
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
import yaml

from datahub.cli.config_utils import load_client_config
from datahub.emitter.token_provider import TokenProviderAuth
from datahub.ingestion.auth.oauth_session import OAuthSessionTokenProvider
from datahub.ingestion.auth.registry import build_token_provider


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
