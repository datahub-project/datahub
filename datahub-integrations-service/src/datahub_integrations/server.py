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
from datahub_integrations.chat.chat_api import router as chat_router
from datahub_integrations.dist import external_router as dist_external_router
from datahub_integrations.gen_ai.description_context import (
    ShellEntityError,
    TooManyColumnsError,
)
from datahub_integrations.gen_ai.router import router as gen_ai_router
from datahub_integrations.notifications.router import router as notifications_router
from datahub_integrations.oauth.router import (
    authenticated_router as oauth_authenticated_router,
    callback_router as oauth_callback_router,
)
from datahub_integrations.share.share_router import router as share_router
from datahub_integrations.slack.slack import (
    SlackLinkPreview,
    get_slack_link_preview,
    private_router as slack_private_router,
    public_router as slack_public_router,
    reload_slack_credentials,
)
from datahub_integrations.sql import router as sql_router
from datahub_integrations.teams.teams import (
    private_router as teams_private_router,
    public_router as teams_public_router,
    reload_teams_credentials,
)
from datahub_integrations.util.access_log import access_log


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Initialize OpenTelemetry observability before other services
    from datahub_integrations.observability import (
        ObservabilityConfig,
        initialize_observability,
    )

    config = None
    try:
        config = ObservabilityConfig()
        initialize_observability(config)
    except Exception as e:
        # Log error but continue - observability should not block service startup
        logging.getLogger(__name__).error(
            f"Failed to initialize observability: {e}. Continuing without metrics."
        )

    # Initialize subprocess metrics bridge for OTLP push (if enabled)
    subprocess_bridge = None
    if config and config.otlp_enabled:
        try:
            from datahub_integrations.observability.subprocess_metrics_bridge import (
                create_subprocess_bridge_if_enabled,
            )

            # Get subprocess URLs from pipeline manager
            def get_subprocess_urls() -> list[str]:
                """Get URLs of all running action subprocesses."""
                try:
                    from datahub_integrations.actions.router import pipeline_manager

                    if pipeline_manager and hasattr(pipeline_manager, "pipelines"):
                        return [
                            f"http://127.0.0.1:{spec.port}"
                            for spec in pipeline_manager.pipelines.values()
                        ]
                except Exception as e:
                    logging.getLogger(__name__).warning(
                        f"Could not get subprocess URLs: {e}"
                    )
                return []

            # Start bridge (will scrape and relay metrics via OTLP)
            subprocess_urls = get_subprocess_urls()
            if subprocess_urls:
                subprocess_bridge = await create_subprocess_bridge_if_enabled(
                    subprocess_urls
                )
                logging.getLogger(__name__).info(
                    f"Subprocess metrics bridge started for {len(subprocess_urls)} subprocess(es)"
                )
            else:
                logging.getLogger(__name__).info(
                    "No action subprocesses running yet, bridge will start when available"
                )
        except Exception as e:
            logging.getLogger(__name__).warning(
                f"Failed to initialize subprocess metrics bridge: {e}"
            )

    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(actions_lifespan(app))

        # Only initialize MCP if not disabled (useful for testing)
        if os.environ.get("DISABLE_MCP", "false").lower() != "true":
            # Lazy import MCP apps to avoid singleton creation at module level
            from datahub_integrations.mcp.router import mcp_http_app, mcp_sse_app

            await stack.enter_async_context(mcp_http_app.lifespan(app))
            await stack.enter_async_context(mcp_sse_app.lifespan(app))

        yield

        # Cleanup subprocess bridge
        if subprocess_bridge:
            try:
                await subprocess_bridge.stop()
                logging.getLogger(__name__).info("Subprocess metrics bridge stopped")
            except Exception as e:
                logging.getLogger(__name__).warning(
                    f"Error stopping subprocess metrics bridge: {e}"
                )


app = FastAPI(lifespan=_lifespan)

# Initialize OpenTelemetry auto-instrumentation for FastAPI
# This must be done after app creation but before adding routes
try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)
except Exception as e:
    logging.getLogger(__name__).warning(
        f"Failed to instrument FastAPI with OpenTelemetry: {e}"
    )

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


@app.get("/metrics")
async def metrics() -> Response:
    """Expose Prometheus-compatible metrics endpoint with subprocess aggregation.

    This endpoint returns metrics in Prometheus text format for scraping.
    It aggregates metrics from:
    1. Main service process (HTTP, system, GenAI metrics)
    2. All running action subprocesses (Phase 3 event/asset processing metrics)

    This allows short-lived processes (e.g., bootstrap) to export metrics before termination,
    while also providing a unified view via scraping.

    Returns:
        Response with aggregated Prometheus text format metrics.
    """
    import httpx

    logger = logging.getLogger(__name__)

    try:
        from prometheus_client import REGISTRY, generate_latest

        # 1. Get main service metrics
        main_metrics_bytes = generate_latest(REGISTRY)
        main_metrics = main_metrics_bytes.decode("utf-8")

        # 2. Scrape metrics from all running action subprocesses
        subprocess_metrics = []

        try:
            # Import here to avoid circular dependency
            from datahub_integrations.actions.router import pipeline_manager

            # Get all running actions
            if pipeline_manager and hasattr(pipeline_manager, "pipelines"):
                for action_urn, spec in pipeline_manager.pipelines.items():
                    try:
                        # Scrape /metrics from action subprocess via its port
                        base_url = f"http://127.0.0.1:{spec.port}"
                        async with httpx.AsyncClient(timeout=2.0) as client:
                            response = await client.get(f"{base_url}/metrics")
                            if response.status_code == 200:
                                subprocess_metrics.append(
                                    f"# Metrics from action: {action_urn}\n{response.text}"
                                )
                            else:
                                logger.warning(
                                    f"Failed to scrape metrics from {action_urn}: HTTP {response.status_code}"
                                )
                    except httpx.TimeoutException:
                        logger.warning(f"Timeout scraping metrics from {action_urn}")
                    except Exception as e:
                        logger.warning(f"Error scraping metrics from {action_urn}: {e}")
        except Exception as e:
            logger.warning(
                f"Could not access pipeline_manager for subprocess metrics: {e}"
            )

        # 3. Combine all metrics
        all_metrics = [main_metrics]
        if subprocess_metrics:
            all_metrics.extend(subprocess_metrics)

        combined_output = "\n\n".join(all_metrics)

        return Response(
            content=combined_output.encode("utf-8"),
            media_type="text/plain; charset=utf-8",
        )
    except Exception as e:
        logger.error(f"Failed to generate metrics: {e}", exc_info=True)
        return Response(
            content=f"# Error generating metrics: {e}\n",
            media_type="text/plain; charset=utf-8",
            status_code=500,
        )


@app.get("/", include_in_schema=False)
def redirect_to_docs() -> Response:
    return RedirectResponse(url="/docs")


@internal_router.post("/reload_credentials")
def reload_credentials() -> None:
    """Reloads all integration credentials from GMS."""

    reload_slack_credentials()
    reload_teams_credentials()


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

# Teams integration routes
internal_router.include_router(teams_private_router, tags=["Teams"])
external_router.include_router(teams_public_router, tags=["Teams"])

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
# Chat router - only include in internal router for now
internal_router.include_router(chat_router, tags=["Chat"])
# OAuth router for AI plugin user authentication
# - authenticated_router: user-facing endpoints (connect, api-key, disconnect)
#   Requires JWT token - security enforced via Depends(get_authenticated_user)
# - callback_router: OAuth callbacks from external providers (no auth required)
# Both are mounted on external_router, accessible via /integrations/ frontend proxy.
external_router.include_router(oauth_authenticated_router, tags=["OAuth"])
external_router.include_router(oauth_callback_router, tags=["OAuth"])

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

# Only mount MCP if not disabled (useful for testing)
if os.environ.get("DISABLE_MCP", "false").lower() != "true":
    # Lazy import MCP apps to avoid singleton creation at module level
    from datahub_integrations.mcp.router import mcp_http_app, mcp_sse_app

    app.mount("/public/ai", mcp_http_app)
    app.mount("/public/ai-legacy", mcp_sse_app)

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
