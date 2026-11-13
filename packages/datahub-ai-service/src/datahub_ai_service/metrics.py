"""Prometheus metrics for operators, served unauthenticated at /metrics.

Labels are bounded: tool names only from the tools this server registered,
endpoints and error kinds from fixed sets. Nothing identifies a caller.
"""

from typing import Callable, Collection, Optional
from urllib.parse import urlsplit

import requests
import urllib3
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    GCCollector,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)
from starlette.requests import Request
from starlette.responses import Response

from datahub_core_mcp_tools.telemetry import ToolCall

METRICS_PATH = "/metrics"

# Tool calls range from a quick lookup to multi-minute lineage walks.
_TOOL_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)
_GMS_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
_OTHER_TOOL = "other"


def _gms_endpoint(url: str) -> str:
    path = urlsplit(url).path
    if path.endswith("/api/graphql"):
        return "graphql"
    if "/openapi/" in path:
        return "openapi"
    if path.endswith("/config"):
        return "config"
    return "other"


def _is_timeout(error: BaseException) -> bool:
    if isinstance(error, requests.Timeout):
        return True
    # With a retry policy mounted, requests reports a read timeout as a
    # ConnectionError wrapping urllib3's MaxRetryError.
    reason = getattr(error.args[0], "reason", None) if error.args else None
    return isinstance(reason, urllib3.exceptions.TimeoutError)


class ServiceMetrics:
    """The service's metrics, in a registry of their own."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        GCCollector(registry=self.registry)
        self.tool_calls = Counter(
            "datahub_ai_mcp_tool_calls",
            "MCP tool calls by tool and outcome (ok, error, cancelled).",
            ["tool", "outcome"],
            registry=self.registry,
        )
        self.tool_call_duration = Histogram(
            "datahub_ai_mcp_tool_call_duration_seconds",
            "How long MCP tool calls took.",
            ["tool"],
            buckets=_TOOL_BUCKETS,
            registry=self.registry,
        )
        self.auth_failures = Counter(
            "datahub_ai_mcp_auth_failures",
            "MCP requests refused before reaching a tool, by reason.",
            ["reason"],
            registry=self.registry,
        )
        self.gms_request_duration = Histogram(
            "datahub_ai_gms_request_duration_seconds",
            "How long requests to GMS took, including retries.",
            ["endpoint"],
            buckets=_GMS_BUCKETS,
            registry=self.registry,
        )
        self.gms_request_errors = Counter(
            "datahub_ai_gms_request_errors",
            "Requests to GMS that failed, by kind (timeout, connection, "
            "rejected, throttled, server_error).",
            ["endpoint", "kind"],
            registry=self.registry,
        )
        self.usage_events_dropped = Counter(
            "datahub_ai_usage_events_dropped",
            "Usage events dropped because the send queue was full.",
            registry=self.registry,
        )

    def tool_call_sink(self, tools: Collection[str]) -> Callable[[ToolCall], None]:
        """A ToolCallRecorder sink counting calls to ``tools``; any other name
        (a client can send anything) is counted as "other"."""
        known = frozenset(tools)

        def record(call: ToolCall) -> None:
            tool = call.tool if call.tool in known else _OTHER_TOOL
            if call.cancelled:
                outcome = "cancelled"
            elif call.result_is_error:
                outcome = "error"
            else:
                outcome = "ok"
            self.tool_calls.labels(tool, outcome).inc()
            self.tool_call_duration.labels(tool).observe(call.duration_seconds)

        return record

    def observe_gms_request(
        self,
        url: str,
        seconds: float,
        *,
        status: Optional[int] = None,
        error: Optional[BaseException] = None,
    ) -> None:
        endpoint = _gms_endpoint(url)
        self.gms_request_duration.labels(endpoint).observe(seconds)
        kind: Optional[str] = None
        if error is not None:
            if _is_timeout(error):
                kind = "timeout"
            elif isinstance(error, requests.RequestException):
                kind = "connection"
        elif status in (401, 403):
            kind = "rejected"
        elif status == 429:
            kind = "throttled"
        elif status is not None and status >= 500:
            kind = "server_error"
        if kind is not None:
            self.gms_request_errors.labels(endpoint, kind).inc()

    async def endpoint(self, request: Request) -> Response:
        return Response(generate_latest(self.registry), media_type=CONTENT_TYPE_LATEST)
