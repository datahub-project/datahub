"""Run synchronous tools off the event loop, inside the caller's request context."""

from collections.abc import Callable
from typing import ParamSpec, TypeVar

import anyio
import anyio.to_thread

from .graphql_helpers import get_mcp_context, with_datahub_client
from .tool_context import BackgroundExecution

_P = ParamSpec("_P")
_R = TypeVar("_R")


async def run_in_background(
    fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
) -> _R:
    """Run ``fn`` in a worker thread.

    anyio copies the calling task's context into the worker, so the per-request
    DataHub client set by the host is the one the tool sees.
    """

    def call() -> _R:
        return fn(*args, **kwargs)

    try:
        context = get_mcp_context()
    except LookupError:
        return await anyio.to_thread.run_sync(call)
    execution = context.tool_context.get(BackgroundExecution)
    if execution is None:
        return await anyio.to_thread.run_sync(call)

    # A parent holds its slot while its inner tools run. Giving those tools a
    # private slot prevents nesting from exhausting the parent's worker budget.
    child_context = context.tool_context.with_items(
        BackgroundExecution(anyio.CapacityLimiter(1))
    )

    def invoke() -> _R:
        with with_datahub_client(
            context.client, child_context, task_context=context.task_context
        ):
            return fn(*args, **kwargs)

    return await anyio.to_thread.run_sync(invoke, limiter=execution.limiter)
