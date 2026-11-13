"""The datahub-ai-service ASGI app: /health, and MCP at /mcp.

Startup never contacts GMS, so the service can start before GMS is healthy;
MCP requests fail closed until GMS answers.
"""

import asyncio
import contextlib
from typing import AsyncIterator, Callable, Optional, Sequence

from loguru import logger
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute, Mount, Route

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo, GmsServerInfoCache
from datahub_core_mcp_tools.telemetry import Ping, sdk_ping

from ._version import __version__
from .catalog import CATALOG_PATH, catalog_endpoint
from .config import AiServiceConfig
from .mcp.auth import DataHubTokenVerifier, McpAuthMiddleware
from .mcp.auth_mode import GmsAuth, GmsAuthDetector
from .mcp.client_factory import CallerClientFactory
from .mcp.feature_gate import McpFeatureGate
from .mcp.host_guard import HostGuard
from .mcp.policy import McpSettingsGate
from .mcp.server import build_mcp_server
from .mcp.settings import McpPolicyCache, SettingsState
from .mcp.telemetry import ToolCallObserver, ping_service_start, product_ping
from .metrics import METRICS_PATH, ServiceMetrics
from .usage_events import UsageEventPost, UsageEventSender

MCP_PATH = "/mcp"


def create_app(
    config: AiServiceConfig,
    *,
    client_factory: Optional[CallerClientFactory] = None,
    server_info: Optional[Callable[[], GmsServerInfo]] = None,
    observers: Sequence[ToolCallObserver] = (),
    telemetry_ping: Ping = sdk_ping,
    usage_event_post: Optional[UsageEventPost] = None,
    policy_cache: Optional[McpPolicyCache] = None,
) -> Starlette:
    """The service's ASGI app.

    ``telemetry_ping`` sends DataHub's anonymous product telemetry (the SDK's,
    off with DATAHUB_TELEMETRY_ENABLED=false). ``usage_event_post`` replaces
    posting usage events to GMS.
    """
    metrics = ServiceMetrics() if config.metrics_enabled else None
    client_factory = client_factory or CallerClientFactory(
        config.gms_url, timeout_seconds=config.gms_timeout_seconds, metrics=metrics
    )
    if server_info is None:
        server_info_cache = GmsServerInfoCache()

        def server_info() -> GmsServerInfo:
            return server_info_cache.get(config.gms_url)

    detector = GmsAuthDetector(
        config.gms_url, server_info, timeout_seconds=config.gms_timeout_seconds
    )
    policy_cache = policy_cache or McpPolicyCache()

    async def health(request: Request) -> JSONResponse:
        # Unauthenticated and never waits on GMS, so container runtimes can probe
        # the process without a credential. Reports the last auth mode and
        # settings state seen, never the settings themselves.
        gms_auth = detector.last
        settings = policy_cache.state
        available = (
            config.mcp_enabled
            and (
                gms_auth is GmsAuth.ON
                or (gms_auth is GmsAuth.OFF and config.auth_mode == "auto")
            )
            and settings in (SettingsState.ENABLED, SettingsState.UNKNOWN)
        )
        return JSONResponse(
            {
                "status": "ok",
                "mcp": {
                    "enabled": config.mcp_enabled,
                    "authMode": config.auth_mode,
                    "gmsAuth": gms_auth.value,
                    "settings": settings.value,
                    "available": available,
                },
            }
        )

    routes: list[BaseRoute] = [Route("/health", health, methods=["GET"])]
    if metrics is not None:
        # Unauthenticated, like /health; the frontend proxies only /mcp.
        routes.append(Route(METRICS_PATH, metrics.endpoint, methods=["GET"]))
    mcp_app = None
    usage_events = (
        UsageEventSender(config.gms_url, metrics=metrics, post=usage_event_post)
        if config.mcp_enabled and config.usage_events_enabled
        else None
    )
    if config.mcp_enabled:
        verifier = DataHubTokenVerifier(
            client_factory, cache_ttl_seconds=config.token_cache_ttl_seconds
        )
        mcp = build_mcp_server(
            client_factory,
            server_info=server_info,
            tool_time_limit_seconds=config.tool_time_limit_seconds,
            on_token_rejected=verifier.invalidate,
            observers=observers,
            telemetry_sinks=[
                product_ping(config, server_info, ping=telemetry_ping),
                *([usage_events] if usage_events is not None else []),
            ],
            metrics=metrics,
        )
        # Unauthenticated, like /health: tool metadata only. The frontend serves
        # it to signed-in users at /ai/catalog/tools.
        routes.append(
            Route(CATALOG_PATH, catalog_endpoint(mcp, server_info), methods=["GET"])
        )
        mcp_app = mcp.http_app(
            path=MCP_PATH,
            transport="http",
            stateless_http=True,
            json_response=config.mcp_response_mode == "json",
            # McpAuthMiddleware enforces its own Host/Origin allowlist, which knows
            # GMS's auth mode and the trusted frontend proxy.
            host_origin_protection=False,
        )
        routes.append(
            Mount(
                "/",
                app=McpAuthMiddleware(
                    McpSettingsGate(
                        mcp_app,
                        policy_cache,
                        client_factory,
                        server_info,
                        gms_url=config.gms_url,
                        on_token_rejected=verifier.invalidate,
                        metrics=metrics,
                    ),
                    verifier,
                    detector,
                    HostGuard(
                        config.allowed_hosts,
                        config.allowed_origins,
                        config.trusted_proxies,
                    ),
                    required=config.auth_mode == "required",
                    metrics=metrics,
                ),
            )
        )

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        logger.info(
            f"datahub-ai-service {__version__} starting: GMS {config.gms_url}, MCP "
            + (f"at {MCP_PATH} ({config.mcp_response_mode})" if mcp_app else "disabled")
        )
        ping_service_start(config, server_info, ping=telemetry_ping)
        if mcp_app is None:
            yield
            return
        try:
            async with mcp_app.lifespan(mcp_app):
                yield
        finally:
            if usage_events is not None:
                await asyncio.to_thread(usage_events.close)

    # MCP_SERVER_ENABLED on GMS turns the MCP paths on and off; /health stays up.
    gate = Middleware(McpFeatureGate, server_info=server_info, paths=(MCP_PATH,))
    return Starlette(routes=routes, lifespan=lifespan, middleware=[gate])
