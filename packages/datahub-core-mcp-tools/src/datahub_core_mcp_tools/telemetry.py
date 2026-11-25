"""Anonymous product telemetry for MCP tool calls, through the DataHub SDK.

Every host (the datahub-ai-service, the stdio wrapper) records tool calls with
``ToolCallRecorder`` and sends one ``mcp-server-tool-call`` event per call with
``TelemetryPing``, carrying the properties the standalone ``mcp-server-datahub``
sent. Events go through the SDK's ``telemetry_instance``, so
``DATAHUB_TELEMETRY_ENABLED=false`` (or a CI environment, or
``datahub telemetry disable``) turns them off.

Events never carry tool arguments, results, error messages or who called: only
the tool's name, the client's name, timings, sizes and the error's class.

Telemetry must never fail or slow a call. Everything that could block (the SDK's
caller detection on its first event, the GMS lookup for server properties) runs
on a background thread behind a bounded queue, which drops events when full.
"""

import dataclasses
import queue
import threading
import time
from typing import Any, Callable, Mapping, Optional, Sequence

import anyio
from fastmcp.server.dependencies import get_http_headers
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

from .gms_server_info import GmsServerInfo

TOOL_CALL_EVENT = "mcp-server-tool-call"
SERVER_START_EVENT = "mcp-server-start"

# 2026-07-28 clients skip the initialize handshake: each request names its
# client in its own _meta instead.
_CLIENT_INFO_META_KEY = "io.modelcontextprotocol/clientInfo"
# Client-supplied strings; bounded so one odd client can't bloat every event.
_MAX_CLIENT_STRING_LENGTH = 200
_MAX_PENDING_EVENTS = 1000

Properties = Mapping[str, Any]
Ping = Callable[[str, dict[str, Any]], None]
Dispatch = Callable[[Callable[[], None]], None]


@dataclasses.dataclass(frozen=True)
class ToolCall:
    """One finished tool call, as telemetry sees it."""

    tool: str
    source: str
    type: str
    method: Optional[str]
    client_name: Optional[str]
    client_version: Optional[str]
    http_user_agent: Optional[str]
    duration_seconds: float
    result_length: Optional[int]
    """Characters of text content returned; None when the call didn't return."""

    error_type: Optional[str]
    """The exception's class name, never its message: messages can carry user data."""

    result_is_error: bool
    cancelled: bool
    """The client cancelled the call or went away before it finished."""

    tags: Optional[frozenset[str]] = None
    """The registered tool's tags; None when no tool has that name."""


def _client_string(value: object) -> Optional[str]:
    if not isinstance(value, str) or not value:
        return None
    return value[:_MAX_CLIENT_STRING_LENGTH]


def client_info(
    context: MiddlewareContext[Any],
) -> tuple[Optional[str], Optional[str]]:
    """The calling MCP client's (name, version), as far as it said.

    Handshake-era clients send clientInfo once, in initialize, and the session
    keeps it (over stdio and stateful HTTP). 2026-era clients send it in each
    request's _meta, so a stateless server sees it on the call itself.
    """
    try:
        ctx = context.fastmcp_context
        request_context = ctx.request_context if ctx is not None else None
        if request_context is None:
            return None, None
        params = getattr(request_context.session, "client_params", None)
        info: Any = getattr(params, "client_info", None)
        if info is None:
            info = (request_context.meta or {}).get(_CLIENT_INFO_META_KEY)
        if isinstance(info, Mapping):
            return _client_string(info.get("name")), _client_string(info.get("version"))
        return (
            _client_string(getattr(info, "name", None)),
            _client_string(getattr(info, "version", None)),
        )
    except Exception:
        logger.opt(exception=True).debug("Could not read the MCP client's info")
        return None, None


def _http_user_agent() -> Optional[str]:
    try:
        # Empty without an HTTP layer (stdio).
        return _client_string(get_http_headers().get("user-agent"))
    except Exception:
        return None


def _text_length(result: Any) -> Optional[int]:
    content = getattr(result, "content", None)
    if not isinstance(content, list):
        return None
    return sum(
        len(text)
        for block in content
        if isinstance(text := getattr(block, "text", None), str)
    )


async def _tool_tags(context: MiddlewareContext[Any]) -> Optional[frozenset[str]]:
    name = getattr(context.message, "name", None)
    if not name or context.fastmcp_context is None:
        return None
    try:
        tool = await context.fastmcp_context.fastmcp.get_tool(name)
    except Exception:
        return None
    return frozenset(tool.tags or ()) if tool is not None else None


class ToolCallRecorder(Middleware):
    """Measures each tool call and hands a ``ToolCall`` to every sink.

    Sinks run on the event loop as the call finishes, still inside its request,
    so they can read request state but must be quick: anything slow belongs on a
    background thread. A failing sink is logged and never reaches the caller.
    """

    def __init__(self, sinks: Sequence[Callable[[ToolCall], None]]) -> None:
        self._sinks = tuple(sinks)

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        if not self._sinks:
            return await call_next(context)

        tags = await _tool_tags(context)
        started = time.perf_counter()
        result: Any = None
        error_type: Optional[str] = None
        cancelled = False
        try:
            result = await call_next(context)
            return result
        except anyio.get_cancelled_exc_class():
            cancelled = True
            raise
        except BaseException as e:
            error_type = type(e).__name__
            raise
        finally:
            self._record(
                context,
                duration_seconds=time.perf_counter() - started,
                result=result,
                error_type=error_type,
                cancelled=cancelled,
                tags=tags,
            )

    def _record(
        self,
        context: MiddlewareContext[Any],
        *,
        duration_seconds: float,
        result: Any,
        error_type: Optional[str],
        cancelled: bool,
        tags: Optional[frozenset[str]],
    ) -> None:
        try:
            name, version = client_info(context)
            call = ToolCall(
                tool=str(getattr(context.message, "name", None) or "unknown"),
                source=context.source,
                type=context.type,
                method=context.method,
                client_name=name,
                client_version=version,
                http_user_agent=_http_user_agent(),
                duration_seconds=duration_seconds,
                result_length=_text_length(result),
                error_type=error_type,
                result_is_error=error_type is not None
                or getattr(result, "is_error", False) is True,
                cancelled=cancelled,
                tags=tags,
            )
        except Exception:
            logger.exception("Could not describe a tool call for telemetry")
            return
        for sink in self._sinks:
            try:
                sink(call)
            except Exception:
                logger.exception(f"Tool-call sink {type(sink).__name__} failed")


class BackgroundDispatcher:
    """Runs tasks one at a time on a daemon thread; drops them when behind."""

    def __init__(
        self,
        max_pending: int = _MAX_PENDING_EVENTS,
        *,
        name: str = "datahub-mcp-telemetry",
        on_drop: Optional[Callable[[], None]] = None,
    ) -> None:
        self._name = name
        self._on_drop = on_drop
        self._queue: queue.Queue[Optional[Callable[[], None]]] = queue.Queue(
            max_pending
        )
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def __call__(self, task: Callable[[], None]) -> None:
        try:
            self._ensure_started()
            self._queue.put_nowait(task)
        except queue.Full:
            logger.debug(f"{self._name} queue full; dropping an event")
            if self._on_drop is not None:
                try:
                    self._on_drop()
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Could not queue a telemetry event: {e}")

    def _ensure_started(self) -> None:
        if self._thread is not None:
            return
        with self._lock:
            if self._thread is None:
                thread = threading.Thread(
                    target=self._run, name=self._name, daemon=True
                )
                thread.start()
                self._thread = thread

    def close(self, timeout_seconds: float = 1.0) -> None:
        """Send what's queued, waiting at most ``timeout_seconds``; then stop."""
        thread = self._thread
        if thread is None:
            return
        try:
            self._queue.put(None, timeout=timeout_seconds)
        except queue.Full:
            return
        thread.join(timeout_seconds)
        with self._lock:
            if self._thread is thread and not thread.is_alive():
                self._thread = None

    def _run(self) -> None:
        while True:
            task = self._queue.get()
            if task is None:
                return
            try:
                task()
            except Exception as e:
                logger.debug(f"Telemetry event failed: {e}")


_background = BackgroundDispatcher()


def sdk_ping(event_name: str, properties: dict[str, Any]) -> None:
    """Send through the SDK's telemetry, which honors DATAHUB_TELEMETRY_ENABLED."""
    from datahub.telemetry import telemetry

    telemetry.telemetry_instance.ping(event_name, properties)


def sdk_telemetry_enabled() -> bool:
    from datahub.telemetry import telemetry

    instance = telemetry.telemetry_instance
    return bool(instance.enabled and instance.mp is not None)


def _always() -> bool:
    return True


def _enabled_check(ping: Ping) -> Callable[[], bool]:
    # Skip building events (and asking GMS for its version) when the SDK would
    # drop them anyway.
    return sdk_telemetry_enabled if ping is sdk_ping else _always


def add_global_property(key: str, value: Any) -> None:
    """Add a property to every SDK telemetry event this process sends."""
    from datahub.telemetry import telemetry

    telemetry.telemetry_instance.add_global_property(key, value)


def gms_server_properties(info: GmsServerInfo) -> dict[str, Any]:
    return {
        "server_version": info.version or "unknown",
        "server_env": info.server_env or "unknown",
        "server_is_cloud": info.is_cloud,
    }


def _server_properties(
    server_info: Optional[Callable[[], GmsServerInfo]],
) -> Properties:
    if server_info is None:
        return {}
    try:
        return gms_server_properties(server_info())
    except Exception:
        # GMS is unreachable: the event still counts, without its server.
        return {"server_version": "unknown", "server_env": "unknown"}


def tool_call_properties(call: ToolCall) -> dict[str, Any]:
    """The legacy server's event properties, minus the error message."""
    properties: dict[str, Any] = {
        "tool": call.tool,
        "source": call.source,
        "type": call.type,
        "method": call.method,
        "duration_seconds": call.duration_seconds,
    }
    optional = {
        "client_name": call.client_name,
        "client_version": call.client_version,
        "http_user_agent": call.http_user_agent,
        "tool_result_length": call.result_length,
        "tool_call_error": call.error_type,
        "tool_call_cancelled": call.cancelled or None,
        "tool_result_is_error": call.result_is_error or None,
    }
    properties.update({k: v for k, v in optional.items() if v is not None})
    return properties


class TelemetryPing:
    """A ``ToolCallRecorder`` sink sending one ``mcp-server-tool-call`` event.

    Args:
        deployment: Which host serves the tools ("datahub-ai-service", "stdio").
        request_properties: More properties, read as the call finishes inside
            its request (e.g. how the caller authenticated). Must be quick.
        server_info: The connected GMS, read on the background thread for the
            ``server_*`` properties.
        ping: Sends one event; the SDK's telemetry by default.
        dispatch: Runs a send off the request path; a bounded background queue
            by default.
    """

    def __init__(
        self,
        *,
        deployment: str,
        request_properties: Optional[Callable[[], Properties]] = None,
        server_info: Optional[Callable[[], GmsServerInfo]] = None,
        ping: Ping = sdk_ping,
        dispatch: Dispatch = _background,
    ) -> None:
        self._deployment = deployment
        self._request_properties = request_properties
        self._server_info = server_info
        self._ping = ping
        self._dispatch = dispatch
        self._enabled = _enabled_check(ping)

    def __call__(self, call: ToolCall) -> None:
        if not self._enabled():
            return
        extra: Properties = {}
        if self._request_properties is not None:
            try:
                extra = self._request_properties()
            except Exception:
                logger.exception("Could not read request properties for telemetry")
        properties = {
            **tool_call_properties(call),
            **extra,
            "deployment": self._deployment,
        }
        ping, server_info = self._ping, self._server_info

        def send() -> None:
            ping(TOOL_CALL_EVENT, {**_server_properties(server_info), **properties})

        self._dispatch(send)


class TelemetryMiddleware(ToolCallRecorder):
    """Sends one ``mcp-server-tool-call`` event per tool call (see TelemetryPing)."""

    def __init__(
        self,
        *,
        deployment: str,
        request_properties: Optional[Callable[[], Properties]] = None,
        server_info: Optional[Callable[[], GmsServerInfo]] = None,
        ping: Ping = sdk_ping,
        dispatch: Dispatch = _background,
    ) -> None:
        super().__init__(
            [
                TelemetryPing(
                    deployment=deployment,
                    request_properties=request_properties,
                    server_info=server_info,
                    ping=ping,
                    dispatch=dispatch,
                )
            ]
        )


def ping_server_start(
    *,
    deployment: str,
    properties: Optional[Properties] = None,
    server_info: Optional[Callable[[], GmsServerInfo]] = None,
    ping: Ping = sdk_ping,
    dispatch: Dispatch = _background,
) -> None:
    """Send one ``mcp-server-start`` event, without waiting for it."""
    if not _enabled_check(ping)():
        return
    fixed = {**(properties or {}), "deployment": deployment}

    def send() -> None:
        ping(SERVER_START_EVENT, {**_server_properties(server_info), **fixed})

    dispatch(send)
