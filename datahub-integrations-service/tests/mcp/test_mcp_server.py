from typing import AsyncIterator

import httpx
import pytest
from asgi_lifespan import LifespanManager

from datahub_integrations.server import app

pytestmark = pytest.mark.anyio
_mcp_route = "/public/ai/mcp"


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
