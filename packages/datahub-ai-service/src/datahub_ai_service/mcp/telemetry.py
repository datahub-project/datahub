"""Where tool-call telemetry plugs in. Observers can never fail or slow a call."""

import dataclasses
import time
from typing import Any, Optional, Protocol, Sequence

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

from .caller import current_caller


@dataclasses.dataclass(frozen=True)
class ToolCallEvent:
    tool_name: str
    actor_urn: Optional[str]
    duration_seconds: float
    error_type: Optional[str]
    """The exception class name when the call failed, never its message: messages
    can carry user data."""


class ToolCallObserver(Protocol):
    def on_tool_call(self, event: ToolCallEvent) -> None: ...


class ToolCallTelemetryMiddleware(Middleware):
    def __init__(self, observers: Sequence[ToolCallObserver]) -> None:
        self._observers = tuple(observers)

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        if not self._observers:
            return await call_next(context)

        started = time.perf_counter()
        error_type: Optional[str] = None
        try:
            return await call_next(context)
        except BaseException as e:
            error_type = type(e).__name__
            raise
        finally:
            caller = current_caller()
            event = ToolCallEvent(
                tool_name=getattr(context.message, "name", "unknown"),
                actor_urn=caller.actor_urn if caller else None,
                duration_seconds=time.perf_counter() - started,
                error_type=error_type,
            )
            for observer in self._observers:
                try:
                    observer.on_tool_call(event)
                except Exception:
                    logger.exception(
                        f"Tool-call observer {type(observer).__name__} failed"
                    )
