from unittest.mock import patch

import requests

from lib.session import PersonaSession, login_persona


def test_persona_session_reuses_same_session() -> None:
    session = requests.Session()
    ps = PersonaSession(
        persona="persona-admin",
        user_urn="urn:li:corpuser:persona-admin",
        frontend_url="http://localhost:9002",
        session=session,
    )
    assert ps.get_session() is session
    assert ps.get_session() is session


@patch("lib.session.get_frontend_session_login_as")
@patch("lib.session.guess_frontend_url_from_gms_url")
def test_login_persona_once(mock_guess, mock_login) -> None:
    mock_guess.return_value = "http://localhost:9002"
    mock_login.return_value = requests.Session()
    ps = login_persona("persona-admin", "persona-admin", "http://localhost:8080")
    assert ps.login_count == 1
    mock_login.assert_called_once()
