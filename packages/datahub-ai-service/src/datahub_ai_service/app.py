"""The datahub-ai-service ASGI app.

Startup never contacts GMS, so the service can start before GMS is healthy.
"""

import contextlib
from typing import AsyncIterator

from loguru import logger
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute, Route

from ._version import __version__
from .config import AiServiceConfig


async def health(request: Request) -> JSONResponse:
    # Unauthenticated and independent of GMS, so container runtimes can probe
    # the process without a credential.
    return JSONResponse({"status": "ok"})


def create_app(config: AiServiceConfig) -> Starlette:
    routes: list[BaseRoute] = [Route("/health", health, methods=["GET"])]

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        logger.info(f"datahub-ai-service {__version__} starting: GMS {config.gms_url}")
        yield

    return Starlette(routes=routes, lifespan=lifespan)
