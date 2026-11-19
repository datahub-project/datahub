"""The datahub-ai-service ASGI app: /health, and MCP at /mcp.

Startup never contacts GMS, so the service can start before GMS is healthy;
MCP requests fail closed until GMS answers.
"""

import contextlib
from typing import AsyncIterator, Optional, Sequence

from loguru import logger
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute, Mount, Route

from ._version import __version__
from .config import AiServiceConfig
from .mcp.auth import CallerClientFactory, DataHubTokenVerifier, McpAuthMiddleware
from .mcp.server import build_mcp_server
from .mcp.telemetry import ToolCallObserver

MCP_PATH = "/mcp"


async def health(request: Request) -> JSONResponse:
    # Unauthenticated and independent of GMS, so container runtimes can probe
    # the process without a credential.
    return JSONResponse({"status": "ok"})


def create_app(
    config: AiServiceConfig,
    *,
    client_factory: Optional[CallerClientFactory] = None,
    observers: Sequence[ToolCallObserver] = (),
) -> Starlette:
    client_factory = client_factory or CallerClientFactory(
        config.gms_url, timeout_seconds=config.gms_timeout_seconds
    )
    routes: list[BaseRoute] = [Route("/health", health, methods=["GET"])]
    mcp_app = None
    if config.mcp_enabled:
        mcp = build_mcp_server(client_factory, observers=observers)
        mcp_app = mcp.http_app(
            path=MCP_PATH,
            transport="http",
            stateless_http=True,
            json_response=config.mcp_response_mode == "json",
        )
        verifier = DataHubTokenVerifier(
            client_factory, cache_ttl_seconds=config.token_cache_ttl_seconds
        )
        routes.append(Mount("/", app=McpAuthMiddleware(mcp_app, verifier)))

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

    return Starlette(routes=routes, lifespan=lifespan)
