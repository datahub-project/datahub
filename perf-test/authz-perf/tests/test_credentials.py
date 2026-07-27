from unittest.mock import patch

import pytest

from lib.credentials import AuthzPerfCredentials, resolve_credentials


def test_uses_datahubenv_token() -> None:
    with patch(
        "lib.credentials._load_datahub_config",
        return_value=("http://localhost:8080", "env-token"),
    ):
        creds = resolve_credentials(allow_prompt=False)
    assert creds == AuthzPerfCredentials(
        gms_url="http://localhost:8080",
        frontend_url="http://localhost:9002",
        token="env-token",
    )


def test_gms_url_cli_overrides_config() -> None:
    with patch(
        "lib.credentials._load_datahub_config",
        return_value=("http://localhost:8080", "env-token"),
    ):
        creds = resolve_credentials(
            gms_url="http://example:8080",
            allow_prompt=False,
        )
    assert creds.gms_url == "http://example:8080"
    assert creds.token == "env-token"


def test_mints_token_from_username_password() -> None:
    with (
        patch(
            "lib.credentials._load_datahub_config",
            return_value=(None, None),
        ),
        patch(
            "lib.credentials.generate_access_token",
            return_value=("urn:li:corpuser:admin", "minted-token"),
        ) as mock_gen,
    ):
        creds = resolve_credentials(
            username="admin",
            password="secret",
            allow_prompt=False,
        )
    assert creds.token == "minted-token"
    mock_gen.assert_called_once_with(
        username="admin",
        password="secret",
        gms_url="http://localhost:8080",
    )


def test_no_credentials_non_interactive_exits() -> None:
    with patch(
        "lib.credentials._load_datahub_config",
        return_value=(None, None),
    ):
        with pytest.raises(SystemExit):
            resolve_credentials(allow_prompt=False)


def test_merges_datahubenv_token_when_env_url_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
    with patch(
        "lib.credentials._load_datahub_config",
        return_value=("http://localhost:8080", "file-token"),
    ):
        creds = resolve_credentials(allow_prompt=False)
    assert creds.token == "file-token"
