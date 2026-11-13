"""Tool-call telemetry: the product ping and the observer seam.

Observers and sinks can never fail or slow a call.
"""

import dataclasses
from typing import Any, Callable, Optional, Protocol, Sequence

from loguru import logger

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.telemetry import (
    Ping,
    TelemetryPing,
    ToolCall,
    ToolCallRecorder,
    add_global_property,
    ping_server_start,
    sdk_ping,
)

from .._version import __version__
from ..config import AiServiceConfig
from .auth_mode import GmsAuth
from .caller import current_caller

DEPLOYMENT = "datahub-ai-service"


@dataclasses.dataclass(frozen=True)
class ToolCallEvent:
    tool_name: str
    actor_urn: Optional[str]
    duration_seconds: float
    error_type: Optional[str]
    """The exception class name when the call failed, never its message: messages
    can carry user data."""

    cancelled: bool = False
    """The client cancelled the call or went away before it finished."""

    call: Optional[ToolCall] = None
    """Everything else recorded about the call."""


class ToolCallObserver(Protocol):
    def on_tool_call(self, event: ToolCallEvent) -> None: ...


class ToolCallTelemetryMiddleware(ToolCallRecorder):
    """Records each tool call for the observers, with the caller's identity,
    and for any further sinks (the product ping)."""

    def __init__(
        self,
        observers: Sequence[ToolCallObserver],
        *,
        sinks: Sequence[Callable[[ToolCall], None]] = (),
    ) -> None:
        self._observers = tuple(observers)
        super().__init__([*([self._notify] if self._observers else []), *sinks])

    def _notify(self, call: ToolCall) -> None:
        caller = current_caller()
        event = ToolCallEvent(
            tool_name=call.tool,
            actor_urn=caller.actor_urn if caller else None,
            duration_seconds=call.duration_seconds,
            error_type=call.error_type,
            cancelled=call.cancelled,
            call=call,
        )
        for observer in self._observers:
            try:
                observer.on_tool_call(event)
            except Exception:
                logger.exception(f"Tool-call observer {type(observer).__name__} failed")


def _auth_properties() -> dict[str, Any]:
    caller = current_caller()
    if caller is None:
        return {}
    # Never the actor: product telemetry is anonymous.
    return {"auth_mode": "token" if caller.gms_auth is GmsAuth.ON else "off"}


def product_ping(
    config: AiServiceConfig,
    server_info: Callable[[], GmsServerInfo],
    *,
    ping: Ping = sdk_ping,
) -> TelemetryPing:
    """The sink that sends each tool call to DataHub's product telemetry."""
    add_global_property("datahub_ai_service_version", __version__)
    response_mode = config.mcp_response_mode

    def request_properties() -> dict[str, Any]:
        return {**_auth_properties(), "response_mode": response_mode}

    return TelemetryPing(
        deployment=DEPLOYMENT,
        request_properties=request_properties,
        server_info=server_info,
        ping=ping,
    )


def ping_service_start(
    config: AiServiceConfig,
    server_info: Callable[[], GmsServerInfo],
    *,
    ping: Ping = sdk_ping,
) -> None:
    add_global_property("datahub_ai_service_version", __version__)
    ping_server_start(
        deployment=DEPLOYMENT,
        properties={
            "mcp_enabled": config.mcp_enabled,
            "response_mode": config.mcp_response_mode,
            "configured_auth_mode": config.auth_mode,
        },
        server_info=server_info,
        ping=ping,
    )
