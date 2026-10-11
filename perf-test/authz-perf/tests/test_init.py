from unittest.mock import patch

from lib.init import smoke_persona_login
from lib.personas import PersonaOracle


@patch("lib.init.execute_graphql")
@patch("lib.init.resolve_operation_document", return_value="query getMe { me { corpUser { urn } } }")
@patch("lib.init.login_persona")
def test_smoke_persona_login(mock_login, mock_load_doc, mock_exec) -> None:
    mock_session = mock_login.return_value
    mock_session.session = mock_session
    mock_session.frontend_url = "http://localhost:9002"
    mock_exec.return_value = __import__(
        "lib.graphql", fromlist=["GraphqlResult"]
    ).GraphqlResult(
        ok=True,
        status_code=200,
        elapsed_ms=1.0,
        data={"data": {"me": {"corpUser": {"urn": "urn:li:corpuser:persona-admin"}}}},
        operation_name="getMe",
    )
    oracle = PersonaOracle(
        user_urn="urn:li:corpuser:persona-admin",
        stress_target="",
        membership_count=0,
    )
    registry = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
    smoke_persona_login(
        "persona-admin",
        oracle,
        "http://localhost:8080",
        registry,
    )
    mock_load_doc.assert_called_once_with(registry, "getMe")
