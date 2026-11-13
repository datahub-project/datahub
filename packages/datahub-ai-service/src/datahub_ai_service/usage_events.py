"""DataHub usage events for MCP tool calls, posted to GMS's tracking endpoint.

Each tool call becomes one ``ToolInvocation`` event in DataHub's own usage
analytics (the ``DataHubUsageEvent`` stream), attributed to the caller. It is
posted with the caller's own credential, and its ``actorUrn`` is the identity
GMS verified for that credential, never anything the client sent. Events carry
no tool arguments or results.

Sending is fire-and-forget: events wait in a bounded queue for a background
sender, are dropped (and counted) when it is full, and a GMS error never
reaches the call. The actor and credential are captured when the call
finishes, inside its own request, so an event can never borrow another
request's identity.
"""

import time
from typing import Any, Callable, Optional

import requests
from loguru import logger

from datahub_core_mcp_tools.mcp_server import ToolType
from datahub_core_mcp_tools.telemetry import BackgroundDispatcher, ToolCall

from ._version import __version__
from .mcp.auth import Caller
from .mcp.auth_mode import GmsAuth
from .mcp.caller import current_caller
from .metrics import ServiceMetrics

TRACK_PATH = "/openapi/v1/tracking/track"
EVENT_TYPE = "ToolInvocation"
# GMS runs every request as its system user while its auth is off.
SYSTEM_ACTOR_URN = "urn:li:corpuser:__datahub_system"
_MAX_PENDING_EVENTS = 1000
_SEND_TIMEOUT_SECONDS = 5.0

UsageEventPost = Callable[[dict[str, Any], Optional[str]], None]
"""Posts one event with the caller's credential (None when they sent none)."""


def actor_for(caller: Caller) -> Optional[str]:
    """Who GMS says made the call, or None when that isn't known."""
    if caller.gms_auth is GmsAuth.ON:
        return caller.actor_urn
    if caller.gms_auth is GmsAuth.OFF:
        return SYSTEM_ACTOR_URN
    return None


def usage_event(call: ToolCall, actor_urn: str, *, timestamp_ms: int) -> dict[str, Any]:
    tags = call.tags or frozenset()
    event: dict[str, Any] = {
        "type": EVENT_TYPE,
        "surface": "mcp",
        "actorUrn": actor_urn,
        "timestamp": timestamp_ms,
        "toolServer": "datahub",
        "toolName": call.tool,
        "toolGroup": "write" if ToolType.MUTATION.value in tags else "read",
        "durationMs": round(call.duration_seconds * 1000),
        "isError": call.result_is_error or call.cancelled,
    }
    optional = {
        "clientName": call.client_name,
        "clientVersion": call.client_version,
        "userAgent": call.http_user_agent,
        "resultLength": call.result_length,
        "errorType": "Cancelled" if call.cancelled else call.error_type,
    }
    event.update({k: v for k, v in optional.items() if v is not None})
    return event


class UsageEventSender:
    """A ToolCallRecorder sink posting one usage event per call to GMS."""

    def __init__(
        self,
        gms_url: str,
        *,
        metrics: Optional[ServiceMetrics] = None,
        max_pending: int = _MAX_PENDING_EVENTS,
        timeout_seconds: float = _SEND_TIMEOUT_SECONDS,
        post: Optional[UsageEventPost] = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._url = f"{gms_url.rstrip('/')}{TRACK_PATH}"
        self._timeout_seconds = timeout_seconds
        self._clock = clock
        self._post = post or self._post_to_gms
        self._session: Optional[requests.Session] = None
        dropped = metrics.usage_events_dropped.inc if metrics is not None else None
        self._dispatch = BackgroundDispatcher(
            max_pending, name="datahub-usage-events", on_drop=dropped
        )

    def __call__(self, call: ToolCall) -> None:
        if call.tags is None:
            # No such tool: the name is whatever the client sent.
            return
        caller = current_caller()
        actor_urn = actor_for(caller) if caller is not None else None
        if caller is None or actor_urn is None:
            return
        event = usage_event(call, actor_urn, timestamp_ms=round(self._clock() * 1000))
        token, post = caller.token, self._post

        def send() -> None:
            post(event, token)

        self._dispatch(send)

    def close(self) -> None:
        self._dispatch.close()
        if self._session is not None:
            self._session.close()

    def _post_to_gms(self, event: dict[str, Any], token: Optional[str]) -> None:
        # Only the worker thread posts, so it can keep one session.
        if self._session is None:
            self._session = requests.Session()
        headers = {"User-Agent": f"datahub-ai-service/{__version__}"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            response = self._session.post(
                self._url, json=event, headers=headers, timeout=self._timeout_seconds
            )
        except requests.RequestException as e:
            logger.debug(f"Could not send a usage event: {type(e).__name__}")
            return
        if response.status_code >= 400:
            logger.debug(f"GMS refused a usage event: HTTP {response.status_code}")
