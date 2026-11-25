"""Report progress while a slow tool runs, for clients that ask for it.

A synchronous tool sends nothing until it returns. When the caller sends a
``_meta.progressToken``, ``with_progress_heartbeat`` reports progress every
``interval`` seconds on the request's stream, so the client sees the call is
alive. Without a token FastMCP drops the notifications.

This is not the transport keepalive: the MCP SDK's SSE responses already send
``: ping`` comments every 15 seconds whether or not the client asked for progress.
"""

import asyncio
import functools
import inspect
import time
from typing import Awaitable, Callable, Optional, ParamSpec, TypeVar

import anyio
from anyio.lowlevel import checkpoint_if_cancelled
from fastmcp.server.context import Context
from fastmcp.server.dependencies import get_context
from loguru import logger

from .background_execution import run_in_background

HEARTBEAT_INTERVAL_SECONDS = 15.0

_P = ParamSpec("_P")
_R = TypeVar("_R")


def with_progress_heartbeat(
    fn: Callable[_P, _R],
    *,
    interval: float = HEARTBEAT_INTERVAL_SECONDS,
    message: str = "Working",
) -> Callable[_P, Awaitable[_R]]:
    """Run a sync tool in a worker thread, reporting progress while it runs.

    A drop-in replacement for ``async_background`` for tools that can run long.
    """
    if inspect.iscoroutinefunction(fn):
        raise RuntimeError("with_progress_heartbeat can only wrap non-async functions")

    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        context = _current_context()
        started = time.monotonic()
        task = asyncio.create_task(run_in_background(fn, *args, **kwargs))
        logged = False
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=interval)
                if done:
                    return task.result()
                if context is not None:
                    await _report(context, time.monotonic() - started, message)
        except Exception:
            # Arguments stay out of the log: they can carry user data.
            logger.exception(f"Tool function {fn.__name__} failed")
            logged = True
            raise
        finally:
            # The worker keeps the caller's client and execution slot until it
            # returns, even when cancellation arrives while reporting progress.
            with anyio.CancelScope(shield=True):
                try:
                    await asyncio.shield(task)
                except Exception:
                    # Logged above unless the caller was cancelled first; then
                    # nobody sees the failure, so leave a trace for operators.
                    if not logged:
                        logger.opt(exception=True).debug(
                            f"Tool function {fn.__name__} failed after its call "
                            "was cancelled"
                        )
            # See async_background: deliver a pending cancellation now.
            await checkpoint_if_cancelled()

    return wrapper


def _current_context() -> Optional[Context]:
    try:
        return get_context()
    except RuntimeError:
        return None


async def _report(context: Context, elapsed: float, message: str) -> None:
    try:
        await context.report_progress(
            progress=round(elapsed), message=f"{message} ({elapsed:.0f}s)"
        )
    except Exception:
        # Best effort: failing to send a heartbeat must not fail the tool.
        logger.opt(exception=True).debug("Progress heartbeat failed")
