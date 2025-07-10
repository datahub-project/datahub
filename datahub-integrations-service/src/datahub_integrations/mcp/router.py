from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from fastapi import Request, Response, status
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse

import datahub_integrations as di
from datahub_integrations.app import DATAHUB_SERVER
from datahub_integrations.mcp.mcp_server import (
    mcp as datahub_fastmcp,
    with_datahub_client,
)


async def _parse_token(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    """ASGI middleware that extracts the token from the request and sets the client context accordingly."""

    # We can get the token from, in order of precedence:
    # 1. the `token` query param
    # 2. the `Authorization: Bearer <token>` header
    token = request.query_params.get("token")
    if not token and (auth_header := request.headers.get("Authorization")):
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                content={
                    "detail": "Invalid authorization header format; expected 'Bearer <token>'"
                },
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        token = auth_header.split(" ", 1)[1]

    # Return an error response if the token is missing or empty.
    # We're explicitly using 400 errors here instead of 401 to ensure that MCP
    # clients don't try to go through an OAuth flow.
    if token is None:
        return JSONResponse(
            content={"detail": "Missing token"},
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    elif not token:
        return JSONResponse(
            content={"detail": "Empty token"}, status_code=status.HTTP_400_BAD_REQUEST
        )

    graph = DataHubGraph(
        config=DatahubClientConfig(
            server=DATAHUB_SERVER,
            token=token,
            datahub_component=f"mcp-server-datahub/hosted {di.__package_name__}/{di.__version__}",
        )
    )
    client = DataHubClient(graph=graph)
    with with_datahub_client(client):
        return await call_next(request)


mcp_http_app = datahub_fastmcp.http_app(
    stateless_http=True,
    middleware=[Middleware(cls=BaseHTTPMiddleware, dispatch=_parse_token)],
)
