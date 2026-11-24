"""MCP through the frontend's /mcp route, served by datahub-ai-service.

The service follows GMS's auth mode, so these tests read it from GMS /config:
with auth on a caller needs a PAT and acts as itself; with auth off no token
is needed, calls run as the system user, and requests that look like DNS
rebinding are refused.
"""

import logging

import pytest
import requests

from tests.e2e.mcp.mcp_client import McpClient
from tests.e2e.utils import execute_graphql, get_frontend_url, get_gms_url
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.AI)

SYSTEM_USER_URN = "urn:li:corpuser:__datahub_system"


@pytest.fixture(scope="module")
def mcp_url() -> str:
    return f"{get_frontend_url()}/mcp"


@pytest.fixture(scope="module")
def gms_auth_enabled() -> bool:
    response = requests.get(f"{get_gms_url()}/config", timeout=30)
    response.raise_for_status()
    enabled = response.json()["authentication"]["enabled"]
    logger.info("GMS authentication enabled: %s", enabled)
    return enabled


@pytest.fixture(scope="module")
def caller_urn(auth_session, gms_auth_enabled: bool) -> str:
    if not gms_auth_enabled:
        return SYSTEM_USER_URN
    # Whoever owns the session's token: the default admin locally, another user
    # when ADMIN_USERNAME or a prebuilt token is set.
    data = execute_graphql(auth_session, "query { me { corpUser { urn } } }")
    return data["data"]["me"]["corpUser"]["urn"]


@pytest.mark.parametrize("modern", [False, True], ids=["legacy", "modern"])
def test_get_me_through_frontend(auth_session, mcp_url, caller_urn, modern):
    client = McpClient(mcp_url, token=auth_session.gms_token(), modern=modern)
    client.connect()

    assert "get_me" in client.list_tool_names()
    me = client.call_tool("get_me")
    assert me["data"]["corpUser"]["urn"] == caller_urn


def test_no_token(mcp_url, gms_auth_enabled):
    client = McpClient(mcp_url)
    if gms_auth_enabled:
        response = client.post("tools/list")
        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"].startswith("Bearer")
    else:
        client.connect()
        me = client.call_tool("get_me")
        assert me["data"]["corpUser"]["urn"] == SYSTEM_USER_URN


def test_garbage_token_rejected(mcp_url, gms_auth_enabled):
    if not gms_auth_enabled:
        pytest.skip("GMS ignores tokens while its authentication is off")
    response = McpClient(mcp_url, token="not-a-token").post("tools/list")
    assert response.status_code == 401


def test_rebinding_request_rejected(mcp_url, gms_auth_enabled):
    if gms_auth_enabled:
        pytest.skip("Host checks guard only the token-free mode")
    response = McpClient(mcp_url).post(
        "tools/list",
        headers={
            "Host": "attacker.example:9002",
            "Origin": "http://attacker.example:9002",
        },
    )
    assert response.status_code == 403
