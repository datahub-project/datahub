from typing import AsyncIterator
from unittest.mock import patch

import httpx
import pytest
from asgi_lifespan import LifespanManager

from datahub_integrations.server import app
from datahub_integrations.telemetry.mcp_events import MCPServerRequestEvent

pytestmark = pytest.mark.anyio
_mcp_route = "/public/ai/mcp"

# This is a fake token that for testing. Created with https://jwt.io/
_fake_token = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyQGV4YW1wbGUuY29tIiwiaWF0IjoxNTE2MjM5MDIyfQ."
    "thisisafakevalue"
)


@pytest.fixture(scope="module")
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with LifespanManager(app) as manager:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=manager.app),  # type: ignore
            base_url="http://example.acryl.io",
            follow_redirects=True,
            headers={"Accept": "application/json, text/event-stream"},
        ) as client:
            yield client


async def test_auth_fails_without_token(client: httpx.AsyncClient) -> None:
    response = await client.post(_mcp_route)
    assert response.status_code == 400
    assert response.json() == {"detail": "Missing token"}


async def test_auth_fails_with_empty_token(client: httpx.AsyncClient) -> None:
    response = await client.post(f"{_mcp_route}?token=")
    assert response.status_code == 400
    assert response.json() == {"detail": "Empty token"}


async def test_auth_fails_with_invalid_header(client: httpx.AsyncClient) -> None:
    response = await client.post(_mcp_route, headers={"Authorization": "Basic test"})
    assert response.status_code == 400
    assert response.json() == {
        "detail": "Invalid authorization header format; expected 'Bearer <token>'"
    }


async def test_ping(client: httpx.AsyncClient) -> None:
    # Just test the ping method of the MCP server.
    # This is mostly a sanity check to make sure the plumbing is set up properly.

    response = await client.post(
        f"{_mcp_route}?token=test",
        json={"jsonrpc": "2.0", "id": "123", "method": "ping"},
    )
    assert response.status_code == 200
    assert (
        response.text
        == 'event: message\r\ndata: {"jsonrpc":"2.0","id":123,"result":{}}\r\n\r\n'
    )


async def test_ping_header_auth(client: httpx.AsyncClient) -> None:
    response = await client.post(
        _mcp_route,
        headers={"Authorization": "Bearer test"},
        json={"jsonrpc": "2.0", "id": "123", "method": "ping"},
    )
    assert response.status_code == 200
    assert (
        response.text
        == 'event: message\r\ndata: {"jsonrpc":"2.0","id":123,"result":{}}\r\n\r\n'
    )


@pytest.mark.parametrize("is_valid_token", [True, False])
async def test_telemetry_middleware_tracks_calls(
    client: httpx.AsyncClient, is_valid_token: bool
) -> None:
    with patch("datahub_integrations.mcp.mcp_telemetry.track_saas_event") as mock_track:
        # The ping method bypasses FastMCP middleware, so we use tools/list instead.
        response = await client.post(
            f"{_mcp_route}?token={_fake_token if is_valid_token else 'invalid'}",
            json={"jsonrpc": "2.0", "id": "123", "method": "tools/list"},
        )
        assert response.status_code == 200

        mock_track.assert_called_once()
        call_args = mock_track.call_args[0][0]
        assert isinstance(call_args, MCPServerRequestEvent)
        assert call_args.source == "client"
        assert call_args.request_type == "request"
        assert call_args.method == "tools/list"
        assert call_args.user_urn == (
            "urn:li:corpuser:user@example.com" if is_valid_token else None
        )
        assert call_args.user_agent is not None and call_args.user_agent.startswith(
            "python-httpx"
        )
        assert call_args.duration_seconds >= 0
        assert call_args.tool_name is None
