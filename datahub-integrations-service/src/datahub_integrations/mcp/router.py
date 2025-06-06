from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from fastapi import Request, Response, status
from starlette.exceptions import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

import datahub_integrations as di
from datahub_integrations.app import DATAHUB_SERVER
from datahub_integrations.mcp.mcp_server import mcp as datahub_fastmcp, with_client


async def _parse_token(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    # Middleware that uses the token query param to set the DataHub client contextvar.
    token = request.query_params.get("token")
    if token is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing token")
    elif not token:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Empty token")

    graph = DataHubGraph(
        config=DatahubClientConfig(
            server=DATAHUB_SERVER,
            token=token,
            datahub_component=f"mcp-server-datahub/hosted {di.__package_name__}/{di.__version__}",
        )
    )
    client = DataHubClient(graph=graph)
    with with_client(client):
        return await call_next(request)


_streamable_http_app = datahub_fastmcp.streamable_http_app()
mcp_session_manager = datahub_fastmcp.session_manager

# Somehow the app.add_middleware() doesn't handle exceptions properly,
# causing 500 errors instead of the intended 400 errors.
# However, using the middleware as a proper onion wrapper works fine.
authed_mcp_app = BaseHTTPMiddleware(app=_streamable_http_app, dispatch=_parse_token)
