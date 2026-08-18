from unittest.mock import MagicMock, patch

from lib.persona_credentials import (
    ensure_persona_passwords,
    is_remote_gms_url,
    set_persona_password,
    should_set_persona_passwords,
)


def test_is_remote_gms_url() -> None:
    assert is_remote_gms_url("https://dev04.acryl.io/gms")
    assert not is_remote_gms_url("http://localhost:8080")


def test_should_set_persona_passwords_auto_remote() -> None:
    assert should_set_persona_passwords(
        "https://dev04.acryl.io/gms",
        env_file=None,
        explicit=None,
    )


def test_should_set_persona_passwords_env_file() -> None:
    assert should_set_persona_passwords(
        "http://localhost:8080",
        env_file="/tmp/.datahubenv_dev04",
        explicit=None,
    )


def test_should_set_persona_passwords_skip() -> None:
    assert not should_set_persona_passwords(
        "https://dev04.acryl.io/gms",
        env_file="/tmp/x",
        explicit=False,
    )


@patch("lib.persona_credentials.requests.Session")
def test_set_persona_password(mock_session_cls: MagicMock) -> None:
    session = MagicMock()
    mock_session_cls.return_value = session
    graphql_resp = MagicMock()
    graphql_resp.json.return_value = {
        "data": {"createNativeUserResetToken": {"resetToken": "tok123"}}
    }
    graphql_resp.raise_for_status = MagicMock()
    reset_resp = MagicMock()
    reset_resp.ok = True
    session.post.side_effect = [graphql_resp, reset_resp]

    set_persona_password(
        frontend_url="https://dev04.acryl.io",
        admin_token="admin-token",
        persona="persona-admin",
        gms_url="https://dev04.acryl.io/gms",
        password="explicit-password",
    )

    assert session.post.call_count == 2
    reset_call = session.post.call_args_list[1]
    assert reset_call[0][0].endswith("/resetNativeUserCredentials")
    assert reset_call[1]["json"]["email"] == "persona-admin"
    assert reset_call[1]["json"]["password"] == "explicit-password"


@patch("lib.persona_credentials.set_persona_password")
def test_ensure_persona_passwords(mock_set: MagicMock) -> None:
    ensure_persona_passwords(
        frontend_url="https://dev04.acryl.io",
        admin_token="admin-token",
        gms_url="https://dev04.acryl.io/gms",
        personas=["persona-admin", "persona-zero-authz"],
    )
    assert mock_set.call_count == 2
    assert mock_set.call_args.kwargs["gms_url"] == "https://dev04.acryl.io/gms"
