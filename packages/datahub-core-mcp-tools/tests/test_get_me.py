from typing import Any
from unittest.mock import MagicMock

import pytest
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.tools.get_me import get_me


def _client(response: Any = None, error: Exception | None = None) -> MagicMock:
    client = MagicMock()
    client._graph.execute_graphql.return_value = response
    client._graph.execute_graphql.side_effect = error
    return client


def _me(**corp_user: Any) -> dict[str, Any]:
    return {
        "me": {
            "corpUser": {
                "type": "CORP_USER",
                "urn": "urn:li:corpuser:jdoe",
                "username": "jdoe",
                **corp_user,
            }
        }
    }


def test_returns_the_callers_user() -> None:
    response = _me(exists=True, info={"displayName": "J Doe"})
    client = _client(response)

    with with_datahub_client(client):
        result = get_me()

    assert result == {
        "success": True,
        "data": response["me"],
        "message": "Successfully retrieved authenticated user information",
    }
    assert client._graph.execute_graphql.call_args.kwargs["operation_name"] == "getMe"


def test_warns_when_datahub_reports_a_nonexistent_user() -> None:
    # What GMS answers with metadata-service auth off and an unknown actor.
    client = _client(_me(urn="urn:li:corpuser:ghost", exists=False))

    with with_datahub_client(client):
        result = get_me()

    assert result["success"] is True
    assert "urn:li:corpuser:ghost" in result["message"]
    assert "METADATA_SERVICE_AUTH_ENABLED" in result["message"]


@pytest.mark.parametrize("response", [{"me": None}, {}])
def test_missing_user_is_a_tool_error(response: dict[str, Any]) -> None:
    with with_datahub_client(_client(response)), pytest.raises(ToolError):
        get_me()


def test_graphql_failure_is_a_tool_error() -> None:
    client = _client(
        error=RuntimeError("401 Unauthorized")
    )  # unknown errors keep their text

    with with_datahub_client(client), pytest.raises(ToolError, match="401"):
        get_me()


def test_needs_a_request_context() -> None:
    with pytest.raises(LookupError):
        get_me()
