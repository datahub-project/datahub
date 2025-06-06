import contextlib
import functools
import logging
import os
from typing import AsyncIterator

import fastapi
import reactpy
import reactpy.backend.fastapi
from fastapi import APIRouter, FastAPI, HTTPException, Response, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from datahub_integrations.actions.actions_manager import (
    InvalidPipelineCommand,
    NoSuchPipeline,
)
from datahub_integrations.actions.router import (
    ACTIONS_ROUTE,
    ActionsAdminUi,
    actions_lifespan,
    actions_router,
)
from datahub_integrations.analytics.router import router as analytics_router
from datahub_integrations.app import STATIC_ASSETS_DIR
from datahub_integrations.dist import external_router as dist_external_router
from datahub_integrations.gen_ai.description_context import (
    ShellEntityError,
    TooManyColumnsError,
)
from datahub_integrations.gen_ai.router import router as gen_ai_router
from datahub_integrations.mcp.router import authed_mcp_app, mcp_session_manager
from datahub_integrations.notifications.router import router as notifications_router
from datahub_integrations.share.share_router import router as share_router
from datahub_integrations.slack.slack import (
    SlackLinkPreview,
    get_slack_link_preview,
    private_router as slack_private_router,
    public_router as slack_public_router,
    reload_slack_credentials,
)
from datahub_integrations.sql import router as sql_router
from datahub_integrations.util.access_log import access_log


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(actions_lifespan(app))
        await stack.enter_async_context(mcp_session_manager.run())
        yield


app = FastAPI(lifespan=_lifespan)

# Disable the uvicorn-native access logger and use our own.
# As per https://github.com/Kludex/asgi-logger#usage
logging.getLogger("uvicorn.access").handlers = []
app.middleware("http")(access_log)

external_router = APIRouter()
internal_router = APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)

_exception_type_mapping = {
    # Actions.
    NoSuchPipeline: status.HTTP_404_NOT_FOUND,
    InvalidPipelineCommand: status.HTTP_400_BAD_REQUEST,
    # Gen AI.
    ShellEntityError: status.HTTP_400_BAD_REQUEST,
    TooManyColumnsError: status.HTTP_422_UNPROCESSABLE_ENTITY,
}


def handle_exception(
    request: fastapi.Request, exc: Exception, status_code: int
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"message": str(exc)},
    )


for exception_type, status_code in _exception_type_mapping.items():
    # Tricky: binding the status code using functools.partial ensures that it is
    # captured at the time of the loop, not at the time of the call.
    app.add_exception_handler(
        exception_type,
        functools.partial(handle_exception, status_code=status_code),
    )


@app.get("/ping")
def ping() -> str:
    return "pong"


@app.get("/", include_in_schema=False)
def redirect_to_docs() -> Response:
    return RedirectResponse(url="/docs")


@internal_router.post("/reload_credentials")
def reload_credentials() -> None:
    """Reloads all integration credentials from GMS."""

    reload_slack_credentials()


class GetLinkPreviewInput(BaseModel):
    type: str
    url: str


@internal_router.post("/get_link_preview", response_model=SlackLinkPreview)
def get_link_preview(input: GetLinkPreviewInput) -> SlackLinkPreview:
    """Get a link preview."""

    if input.type == "SLACK_MESSAGE":
        return get_slack_link_preview(input.url)
    else:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, f"Unknown link type: {input.type}"
        )


# Note: the slack routes currently all have /slack hardcoded in them,
# but eventually we should move the `/slack` prefix here.
internal_router.include_router(slack_private_router, tags=["Slack"])
external_router.include_router(slack_public_router, tags=["Slack"])

internal_router.include_router(actions_router, prefix=ACTIONS_ROUTE, tags=["Actions"])
internal_router.include_router(gen_ai_router, prefix="/ai", tags=["AI"])
internal_router.include_router(
    analytics_router, prefix="/analytics", tags=["Analytics"]
)
internal_router.include_router(
    notifications_router, prefix="/notifications", tags=["Notifications"]
)
internal_router.include_router(share_router, prefix="/share", tags=["Share"])
internal_router.include_router(sql_router, prefix="/sql", tags=["SQL"])

external_router.include_router(dist_external_router, tags=["Dist"])

# Using .mount() on ApiRouter doesn't work. See
# https://github.com/tiangolo/fastapi/issues/1469 and
# https://github.com/fastapi/fastapi/issues/10180.
# As such, we have to mount sub-applications directly on the main app.
app.mount(
    "/public/static",
    StaticFiles(directory=STATIC_ASSETS_DIR),
    name="integrations-static-dir",
)
app.mount("/public/ai", authed_mcp_app)


app.include_router(internal_router, prefix="/private")
app.include_router(external_router, prefix="/public")

if os.environ.get("DEV_MODE_BYPASS_FRONTEND", False):
    # This is only for development purposes.
    # When running this directly instead of through the frontend server's proxy, we
    # need to mount the external router on /integrations because that's what the frontend
    # route is.
    app.include_router(external_router, prefix="/integrations")

# ReactPy UI.
reactpy.backend.fastapi.configure(
    app,
    ActionsAdminUi,
    reactpy.backend.fastapi.Options(
        url_prefix="/ui",
        head=[
            reactpy.html.link(
                {
                    "rel": "stylesheet",
                    "type": "text/css",
                    "href": "https://unpkg.com/semantic-ui@2.5.0/dist/semantic.min.css",
                    "crossorigin": "anonymous",
                }
            ),
            reactpy.html.script(
                {
                    "src": "https://code.jquery.com/jquery-3.1.1.min.js",
                    "integrity": "sha256-hVVnYaiADRTO2PzUGmuLJr8BLUSjGIZsDYGmIJLv2b8=",
                    "crossorigin": "anonymous",
                }
            ),
            reactpy.html.script(
                {
                    "src": "https://unpkg.com/semantic-ui@2.5.0/dist/semantic.min.js",
                    "crossorigin": "anonymous",
                }
            ),
        ],
    ),
)


def use_route_names_as_operation_ids(app: fastapi.FastAPI) -> None:
    # See https://fastapi.tiangolo.com/advanced/path-operation-advanced-configuration/#using-the-path-operation-function-name-as-the-operationid
    for route in app.routes:
        if isinstance(route, fastapi.routing.APIRoute):
            route.operation_id = route.name


use_route_names_as_operation_ids(app)
