import asyncio
import threading
import time
from typing import Any, Callable
from unittest.mock import MagicMock

import anyio
import anyio.from_thread
import pytest
from fastmcp import Client, Context, FastMCP
from loguru import logger
from mcp.shared.exceptions import MCPError

from datahub_core_mcp_tools.graphql_helpers import (
    get_datahub_client,
    with_datahub_client,
)
from datahub_core_mcp_tools.mcp_server import async_background
from datahub_core_mcp_tools.progress_heartbeat import with_progress_heartbeat


def _blocking_worker(
    started: anyio.Event, release: threading.Event, raises: bool
) -> Callable[[], str]:
    def worker() -> str:
        anyio.from_thread.run_sync(started.set)
        if not release.wait(timeout=5):
            raise TimeoutError("Worker was not released")
        if raises:
            raise ValueError("Worker failed")
        return "done"

    return worker


@pytest.mark.parametrize("worker_raises", [False, True])
async def test_cancellation_waits_for_the_worker_then_is_delivered(
    worker_raises: bool,
) -> None:
    started, finished = anyio.Event(), anyio.Event()
    release = threading.Event()
    scope = anyio.CancelScope()
    outcomes: list[str] = []
    worker = async_background(_blocking_worker(started, release, worker_raises))

    async def run_worker() -> None:
        with scope:
            try:
                outcomes.append(await worker())
            except ValueError:
                outcomes.append("error")
        finished.set()

    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(run_worker)
            try:
                await started.wait()
                scope.cancel()
                await anyio.sleep(0)
                # The worker still owns its client and slot: cancellation waits.
                assert not finished.is_set()
            finally:
                release.set()
            await finished.wait()

    assert scope.cancelled_caught
    assert outcomes == []


async def test_uncancelled_worker_error_is_raised() -> None:
    def worker() -> None:
        raise ValueError("Worker failed")

    with pytest.raises(ValueError, match="Worker failed"):
        await async_background(worker)()


@pytest.mark.parametrize("worker_raises", [False, True])
async def test_session_survives_a_cancelled_background_tool(
    worker_raises: bool,
) -> None:
    server = FastMCP("cancellation")
    started = anyio.Event()
    release = threading.Event()
    request_id: Any = None
    worker = async_background(_blocking_worker(started, release, worker_raises))

    @server.tool
    async def slow_tool(ctx: Context) -> str:
        nonlocal request_id
        assert ctx.request_context is not None
        request_id = ctx.request_context.request_id
        return await worker()

    @server.tool
    def echo() -> str:
        return "ok"

    with anyio.fail_after(10):
        async with Client(server, mode="legacy") as client:
            pending = asyncio.create_task(client.call_tool("slow_tool"))
            try:
                await started.wait()
                await client.cancel(request_id, reason="test")
                await anyio.sleep(0.1)
            finally:
                # The worker can't be interrupted; the call ends once it returns.
                release.set()
            # The server answers nothing for a cancelled request (as the spec
            # says), so the client stops waiting on its own.
            await anyio.sleep(0.2)
            pending.cancel()
            with pytest.raises((asyncio.CancelledError, MCPError)):
                await pending
            # The session survives: the cancelled call sent no stray response.
            assert (await client.call_tool("echo")).data == "ok"


def _slow_server(seconds: float, interval: float) -> FastMCP:
    server = FastMCP("heartbeat")

    def slow() -> str:
        time.sleep(seconds)
        return str(get_datahub_client())

    tool = with_progress_heartbeat(slow, interval=interval)

    @server.tool
    async def slow_tool() -> str:
        with with_datahub_client(MagicMock(__str__=lambda _: "caller-client")):
            return await tool()

    return server


async def test_heartbeat_reports_progress_to_clients_that_ask() -> None:
    progress: list[str] = []

    async def on_progress(value: float, total: Any, message: Any) -> None:
        progress.append(message)

    async with Client(_slow_server(0.5, 0.1), progress_handler=on_progress) as client:
        result = await client.call_tool("slow_tool")

    assert result.data == "caller-client"
    assert len(progress) >= 2
    assert all(m.startswith("Working (") for m in progress)


async def test_heartbeat_is_silent_without_a_progress_request() -> None:
    async with Client(_slow_server(0.3, 0.05)) as client:
        assert (await client.call_tool("slow_tool")).data == "caller-client"


async def test_heartbeat_logs_a_failure_after_the_call_was_cancelled() -> None:
    started = anyio.Event()
    release = threading.Event()
    scope = anyio.CancelScope()
    tool = with_progress_heartbeat(
        _blocking_worker(started, release, raises=True), interval=0.05
    )
    messages: list[str] = []
    handler = logger.add(messages.append, level="DEBUG", format="{message}")

    async def call() -> None:
        with scope:
            await tool()

    try:
        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(call)
                await started.wait()
                scope.cancel()
                await anyio.sleep(0)
                release.set()
    finally:
        release.set()
        logger.remove(handler)

    assert scope.cancelled_caught
    assert any("failed after its call was cancelled" in m for m in messages)


def test_heartbeat_rejects_async_functions() -> None:
    async def already_async() -> None: ...

    with pytest.raises(RuntimeError):
        with_progress_heartbeat(already_async)
