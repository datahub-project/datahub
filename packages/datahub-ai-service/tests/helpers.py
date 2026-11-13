import asyncio
import contextlib
import dataclasses
import threading
import time
from typing import Any, AsyncIterator, Optional

import requests
import uvicorn
from datahub.configuration.common import OperationalError
from starlette.applications import Starlette

from datahub_ai_service.mcp.client_factory import CallerClientFactory
from datahub_core_mcp_tools.gms_server_info import GmsServerInfo


def rejected_token_error(status: int = 401) -> Exception:
    # How the SDK reports a GMS 401: an OperationalError chained from requests'.
    response = requests.Response()
    response.status_code = status
    try:
        raise OperationalError("Unable to get metadata from DataHub") from (
            requests.HTTPError(response=response)
        )
    except OperationalError as e:
        return e


@dataclasses.dataclass
class FakeGms:
    """Stands in for GMS: knows which tokens belong to which users."""

    users: dict[str, str] = dataclasses.field(
        default_factory=lambda: {
            "alice-token": "urn:li:corpuser:alice",
            "bob-token": "urn:li:corpuser:bob",
        }
    )
    down: bool = False
    auth_enabled: Optional[bool] = True
    """What /config reports; None is a GMS from before it reported it."""
    mcp_server_enabled: Optional[bool] = True
    """MCP_SERVER_ENABLED as /config reports it; None is a GMS from before the flag."""
    me_calls: int = 0
    open_clients: int = 0
    # When set, every getMe call waits here, so concurrent calls overlap.
    get_me_barrier: Optional[threading.Barrier] = None
    get_me_seconds: float = 0.0
    mcp_settings: Any = dataclasses.field(
        default_factory=lambda: {"enabled": True, "defaultMcpServerOverrides": None}
    )
    """What GraphQL mcpSettings returns."""
    settings_error: Optional[Exception] = None
    """Raised by every mcpSettings read when set."""
    settings_denied_tokens: set[str] = dataclasses.field(default_factory=set)
    """Callers GMS refuses the settings to, with a 403."""
    settings_calls: int = 0

    def server_info(self) -> GmsServerInfo:
        if self.down:
            raise requests.ConnectionError("connection refused")
        return GmsServerInfo(
            version="v1.7.0",
            server_env="core",
            auth_enabled=self.auth_enabled,
            mcp_server_enabled=self.mcp_server_enabled,
        )

    def execute_graphql(
        self, token: Optional[str], operation_name: Optional[str]
    ) -> Any:
        if self.down:
            raise requests.ConnectionError("connection refused")
        if self.auth_enabled is False:
            # GMS's NoOpAuthenticator: any token, or none, is the system user.
            token = None
        elif token not in self.users:
            raise rejected_token_error()
        if operation_name == "getMcpSettings":
            self.settings_calls += 1
            if self.settings_error is not None:
                raise self.settings_error
            if token in self.settings_denied_tokens:
                raise rejected_token_error(403)
            return {"mcpSettings": self.mcp_settings}
        self.me_calls += 1
        if operation_name == "getMe" and self.get_me_barrier:
            self.get_me_barrier.wait()
        if operation_name == "getMe":
            time.sleep(self.get_me_seconds)
        urn = self.users[token] if token else "urn:li:corpuser:__datahub_system"
        return {
            "me": {
                "corpUser": {
                    "urn": urn,
                    "username": urn.rsplit(":", 1)[-1],
                    "exists": True,
                }
            }
        }


class _FakeGraph:
    def __init__(self, gms: FakeGms, token: Optional[str]) -> None:
        self._gms = gms
        self._token = token

    def execute_graphql(
        self, query: str, operation_name: Optional[str] = None, **_: Any
    ) -> Any:
        return self._gms.execute_graphql(self._token, operation_name)

    def close(self) -> None:
        self._gms.open_clients -= 1


class FakeClientFactory(CallerClientFactory):
    def __init__(self, gms: FakeGms) -> None:
        self._gms = gms

    def build(
        self,
        token: Optional[str],
        *,
        time_limit_seconds: Optional[float] = None,
        on_rejected: Any = None,
    ) -> Any:
        self._gms.open_clients += 1
        client = type("FakeClient", (), {})()
        client._graph = _FakeGraph(self._gms, token)
        return client


@contextlib.asynccontextmanager
async def serve(app: Starlette) -> AsyncIterator[str]:
    """Run ``app`` on a real HTTP server for the duration of the block."""
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    )
    task = asyncio.create_task(server.serve())
    while not server.started:
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task
