"""The datahub-ai-service ASGI app: /health, and MCP at /mcp.

Startup never contacts GMS, so the service can start before GMS is healthy;
MCP requests fail closed until GMS answers.
"""

import contextlib
from typing import AsyncIterator, Callable, Optional, Sequence

from loguru import logger
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute, Mount, Route

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo, GmsServerInfoCache

from ._version import __version__
from .config import AiServiceConfig
from .mcp.auth import DataHubTokenVerifier, McpAuthMiddleware
from .mcp.auth_mode import GmsAuth, GmsAuthDetector
from .mcp.client_factory import CallerClientFactory
from .mcp.feature_gate import McpFeatureGate
from .mcp.host_guard import HostGuard
from .mcp.server import build_mcp_server
from .mcp.telemetry import ToolCallObserver

MCP_PATH = "/mcp"


def create_app(
    config: AiServiceConfig,
    *,
    client_factory: Optional[CallerClientFactory] = None,
    server_info: Optional[Callable[[], GmsServerInfo]] = None,
    observers: Sequence[ToolCallObserver] = (),
) -> Starlette:
    client_factory = client_factory or CallerClientFactory(
        config.gms_url, timeout_seconds=config.gms_timeout_seconds
    )
    if server_info is None:
        server_info_cache = GmsServerInfoCache()

        def server_info() -> GmsServerInfo:
            return server_info_cache.get(config.gms_url)

    detector = GmsAuthDetector(
        config.gms_url, server_info, timeout_seconds=config.gms_timeout_seconds
    )

    async def health(request: Request) -> JSONResponse:
        # Unauthenticated and never waits on GMS, so container runtimes can probe
        # the process without a credential. Reports the last auth mode seen.
        gms_auth = detector.last
        available = config.mcp_enabled and (
            gms_auth is GmsAuth.ON
            or (gms_auth is GmsAuth.OFF and config.auth_mode == "auto")
        )
        return JSONResponse(
            {
                "status": "ok",
                "mcp": {
                    "enabled": config.mcp_enabled,
                    "authMode": config.auth_mode,
                    "gmsAuth": gms_auth.value,
                    "available": available,
                },
            }
        )

    routes: list[BaseRoute] = [Route("/health", health, methods=["GET"])]
    mcp_app = None
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
                    mcp_app,
                    verifier,
                    detector,
                    HostGuard(
                        config.allowed_hosts,
                        config.allowed_origins,
                        config.trusted_proxies,
                    ),
                    required=config.auth_mode == "required",
                ),
            )
        )

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        logger.info(
            f"datahub-ai-service {__version__} starting: GMS {config.gms_url}, MCP "
            + (f"at {MCP_PATH} ({config.mcp_response_mode})" if mcp_app else "disabled")
        )
        if mcp_app is None:
            yield
            return
        async with mcp_app.lifespan(mcp_app):
            yield

    # MCP_SERVER_ENABLED on GMS turns the MCP paths on and off; /health stays up.
    gate = Middleware(McpFeatureGate, server_info=server_info, paths=(MCP_PATH,))
    return Starlette(routes=routes, lifespan=lifespan, middleware=[gate])
