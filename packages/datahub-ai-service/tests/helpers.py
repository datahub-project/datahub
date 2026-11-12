import asyncio
import contextlib
import dataclasses
import threading
from typing import Any, AsyncIterator, Optional

import requests
import uvicorn
from datahub.configuration.common import OperationalError
from starlette.applications import Starlette

from datahub_ai_service.mcp.auth import CallerClientFactory


def rejected_token_error() -> Exception:
    # How the SDK reports a GMS 401: an OperationalError chained from requests'.
    response = requests.Response()
    response.status_code = 401
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
    me_calls: int = 0
    open_clients: int = 0
    # When set, every getMe call waits here, so concurrent calls overlap.
    get_me_barrier: Optional[threading.Barrier] = None

    def execute_graphql(self, token: str, operation_name: Optional[str]) -> Any:
        if self.down:
            raise requests.ConnectionError("connection refused")
        if token not in self.users:
            raise rejected_token_error()
        self.me_calls += 1
        if operation_name == "getMe" and self.get_me_barrier:
            self.get_me_barrier.wait()
        urn = self.users[token]
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
    def __init__(self, gms: FakeGms, token: str) -> None:
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

    def build(self, token: str) -> Any:
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
