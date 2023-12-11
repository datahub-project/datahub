import os

import fastapi
from fastapi import HTTPException, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from datahub_integrations.actions.router import ACTIONS_ROUTE, actions_router
from datahub_integrations.app import (
    STATIC_ASSETS_DIR,
    app,
    external_router,
    internal_router,
)
from datahub_integrations.gen_ai.router import router as gen_ai_router
from datahub_integrations.slack.slack import SlackLinkPreview, get_slack_link_preview
from datahub_integrations.slack.slack import private_router as slack_private_router
from datahub_integrations.slack.slack import public_router as slack_public_router
from datahub_integrations.slack.slack import reload_slack_credentials


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

app.include_router(internal_router, prefix="/private")

# Using .mount() on ApiRouter doesn't work. See https://github.com/tiangolo/fastapi/issues/1469.
# As such, we have to mount it directly on the app.
app.mount(
    "/public/static",
    StaticFiles(directory=STATIC_ASSETS_DIR),
    name="integrations-static-dir",
)
app.include_router(external_router, prefix="/public")

if os.environ.get("DEV_MODE_BYPASS_FRONTEND", False):
    # This is only for development purposes.
    # When running this directly instead of through the frontend server's proxy, we
    # need to mount the external router on /integrations because that's what the frontend
    # route is.
    app.include_router(external_router, prefix="/integrations")


def use_route_names_as_operation_ids(app: fastapi.FastAPI) -> None:
    # See https://fastapi.tiangolo.com/advanced/path-operation-advanced-configuration/#using-the-path-operation-function-name-as-the-operationid
    for route in app.routes:
        if isinstance(route, fastapi.routing.APIRoute):
            route.operation_id = route.name


use_route_names_as_operation_ids(app)
