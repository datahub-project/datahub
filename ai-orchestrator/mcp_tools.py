"""Tools come from DataHub's MCP server (mcp-server-datahub), not hand-written GraphQL.
Transport is HTTP if DATAHUB_MCP_URL is set, else stdio via `uvx mcp-server-datahub`."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

logger = logging.getLogger("mcp_tools")

DATAHUB_MCP_URL = os.environ.get("DATAHUB_MCP_URL", "")
DATAHUB_MCP_TOKEN = os.environ.get("DATAHUB_MCP_TOKEN", "")

# stdio transport only: the GMS the spawned server talks to.
DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN", "")
# Opt-in to write tools (tags/descriptions/owners/...).
ENABLE_MUTATIONS = os.environ.get("DATAHUB_MCP_ENABLE_MUTATIONS", "").lower() in (
    "1",
    "true",
    "yes",
)

_TOOL_CALL_TIMEOUT = float(os.environ.get("DATAHUB_MCP_TOOL_TIMEOUT", "60"))
_RECONNECT_DELAY = 2.0


def _stdio_params() -> StdioServerParameters:
    env = {**os.environ, "DATAHUB_GMS_URL": DATAHUB_GMS_URL}
    if DATAHUB_GMS_TOKEN:
        env["DATAHUB_GMS_TOKEN"] = DATAHUB_GMS_TOKEN
    if ENABLE_MUTATIONS:
        env["TOOLS_IS_MUTATION_ENABLED"] = "true"
    # If `uvx` isn't on PATH you'll get `spawn uvx ENOENT` — use its full path here.
    return StdioServerParameters(
        command="uvx", args=["mcp-server-datahub@latest"], env=env
    )


@asynccontextmanager
async def _connect() -> AsyncIterator[ClientSession]:
    if DATAHUB_MCP_URL:
        headers = (
            {"Authorization": f"Bearer {DATAHUB_MCP_TOKEN}"} if DATAHUB_MCP_TOKEN else None
        )
        async with streamablehttp_client(DATAHUB_MCP_URL, headers=headers) as (read, write, _):
            async with ClientSession(read, write) as session:
                yield session
    else:
        async with stdio_client(_stdio_params()) as (read, write):
            async with ClientSession(read, write) as session:
                yield session


def _collapse(result: Any) -> dict[str, Any]:
    text = "\n".join(
        block.text for block in result.content if getattr(block, "type", None) == "text"
    )
    if result.isError:
        return {"error": text or "Tool failed"}
    return {"result": text}


class PersistentMCP:
    """Long-lived MCP session owned by one background task; callers queue tool calls
    to it, so session I/O stays in a single task (a ClientSession isn't task-safe)."""

    def __init__(self) -> None:
        self.definitions: list[dict[str, Any]] = []
        self._queue: asyncio.Queue = asyncio.Queue()
        self._connected = asyncio.Event()
        self._stop = False
        self._task: Optional[asyncio.Task] = None

    async def start(self, connect_timeout: float = 20.0) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="mcp-owner")
        # Don't block startup if GMS is briefly unreachable; _run keeps retrying.
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=connect_timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "MCP not connected after %ss; continuing (will retry in background).",
                connect_timeout,
            )

    async def stop(self) -> None:
        self._stop = True
        await self._queue.put(None)  # wake the serve loop
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    async def _run(self) -> None:
        while not self._stop:
            try:
                async with _connect() as session:
                    await session.initialize()
                    tools = (await session.list_tools()).tools
                    self.definitions = [
                        {
                            "name": t.name,
                            "description": t.description or "",
                            "input_schema": t.inputSchema,
                        }
                        for t in tools
                    ]
                    self._connected.set()
                    logger.info("MCP connected (%d tools).", len(self.definitions))
                    await self._serve(session)
            except Exception as exc:  # noqa: BLE001
                if self._stop:
                    break
                self._connected.clear()
                logger.warning("MCP session dropped (%s); reconnecting in %ss.", exc, _RECONNECT_DELAY)
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _serve(self, session: ClientSession) -> None:
        # Tool errors arrive as isError results; an exception here means the session
        # is broken, so let it propagate to _run and trigger a reconnect.
        while not self._stop:
            item = await self._queue.get()
            if item is None:
                return
            name, args, fut = item
            if fut.done():  # caller already timed out
                continue
            result = await session.call_tool(name, args or {})
            if not fut.done():
                fut.set_result(_collapse(result))

    async def execute_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        if not self._connected.is_set():
            try:
                await asyncio.wait_for(self._connected.wait(), timeout=_TOOL_CALL_TIMEOUT)
            except asyncio.TimeoutError:
                return {"error": f"Tool {name} failed: MCP server not connected"}
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._queue.put((name, args, fut))
        try:
            return await asyncio.wait_for(fut, timeout=_TOOL_CALL_TIMEOUT)
        except asyncio.TimeoutError:
            return {"error": f"Tool {name} timed out after {_TOOL_CALL_TIMEOUT}s"}
        except Exception as exc:  # noqa: BLE001
            return {"error": f"Tool {name} failed: {exc}"}


_manager: Optional[PersistentMCP] = None
_manager_lock = asyncio.Lock()


async def get_mcp() -> PersistentMCP:
    global _manager
    if _manager is None:
        async with _manager_lock:
            if _manager is None:
                manager = PersistentMCP()
                await manager.start()
                _manager = manager
    return _manager


async def shutdown_mcp() -> None:
    global _manager
    if _manager is not None:
        await _manager.stop()
        _manager = None
