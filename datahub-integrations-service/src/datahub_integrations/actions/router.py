import contextlib
import json
import pathlib
from typing import AsyncIterator

import fastapi
import httpx
import starlette
import starlette.background
from fastapi import HTTPException, status
from loguru import logger

from datahub_integrations.actions.actions_manager import ActionsManager, ActionSpec
from datahub_integrations.app import DATAHUB_SERVER, graph

ACTIONS_ROUTE = "/actions"
actions_router = fastapi.APIRouter()

actions_gql = (pathlib.Path(__file__).parent / "actions.gql").read_text()


base_action_config = {
    "name": "hello_world",
    "source": {
        "type": "kafka",
        "config": {
            "connection": {
                "bootstrap": "${KAFKA_BOOTSTRAP_SERVER:-broker:29092}",
                "schema_registry_url": "${SCHEMA_REGISTRY_URL:-http://datahub-gms:8080/schema-registry/api/}",
            }
        },
    },
    "datahub": {
        "server": DATAHUB_SERVER,
    },
    "filter": {
        "event_type": "EntityChangeEvent_v1",
    },
}


pipeline_manager = ActionsManager()
_httpx_client = httpx.AsyncClient()


@contextlib.asynccontextmanager
async def actions_lifespan(app: fastapi.FastAPI) -> AsyncIterator[None]:
    async with pipeline_manager:
        logger.debug("Fetching registered actions.")

        all_actions = graph.execute_graphql(
            query=actions_gql, operation_name="listActions"
        )
        logger.debug(f"Got actions: {all_actions}")

        try:
            for action in all_actions["listActionPipelines"]["actionPipelines"]:
                action_urn = action["urn"]
                action_details = action["details"]

                logger.info(f"Starting action {action_urn}.")
                await start_or_restart_action(action_urn, action_details)

            yield

        finally:
            logger.info("Stopping all running actions.")
            await pipeline_manager.stop_all()

            logger.info("All actions stopped.")


@actions_router.get("/")
def list_running_actions() -> list[str]:
    """Get all running actions."""

    return list(pipeline_manager.pipelines.keys())


async def start_or_restart_action(action_urn: str, action_details: dict) -> None:
    if pipeline_manager.is_running(action_urn):
        logger.info(f"Stopping action {action_urn}.")
        await pipeline_manager.stop_pipeline(action_urn)

    action_details_recipe = json.loads(action_details["config"]["recipe"])
    # TODO respect version and other details

    recipe = {
        **base_action_config,
        "name": action_urn,
        **action_details_recipe,
    }

    logger.debug(f"Starting action {action_urn} with recipe: {recipe}")

    try:
        await pipeline_manager.start_pipeline(action_urn, recipe)
    except Exception as e:
        logger.error(f"Failed to register action {action_urn}: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e)) from e


@actions_router.post("/{action_urn}/reload")
async def reload_action(action_urn: str) -> None:
    """
    Reload an action.

    If the action was not yet running, it will be started.
    """

    logger.info(f"Reloading action {action_urn}.")

    updated_details = graph.execute_graphql(
        actions_gql, operation_name="getAction", variables={"urn": action_urn}
    )["actionPipeline"]["details"]

    await start_or_restart_action(action_urn, updated_details)


def _get_action_spec(action_urn: str) -> ActionSpec:
    if action_urn not in pipeline_manager.pipelines:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"Action {action_urn} not found."
        )

    return pipeline_manager.pipelines[action_urn]


@actions_router.post("/{action_urn}/stop")
async def stop_action(action_urn: str) -> str:
    """
    Stop an action.

    This is mainly for debugging purposes.
    """

    _get_action_spec(action_urn)

    try:
        await pipeline_manager.stop_pipeline(action_urn)
        return "OK"
    except Exception as e:
        logger.error(f"Failed to stop action {action_urn}: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e)) from e


@actions_router.get("/{action_urn}/live_logs")
async def action_logs(action_urn: str) -> fastapi.Response:
    """Get the most recent logs from the action."""

    spec = _get_action_spec(action_urn)

    return fastapi.responses.PlainTextResponse(content=spec.logs.get_logs())


@actions_router.get("/{action_urn}/ping")
async def action_ping(action_urn: str) -> str:
    """Call the ping endpoint of the action."""

    spec = _get_action_spec(action_urn)

    res = await _httpx_client.get(f"{spec.base_url}/ping")

    return res.text


@actions_router.get("/{action_urn}/stats")
async def action_stats(action_urn: str) -> dict:
    """Call the stats endpoint of the action."""

    spec = _get_action_spec(action_urn)

    res = await _httpx_client.get(f"{spec.base_url}/stats")

    return res.json()


@actions_router.api_route(
    "/{action_urn}/proxy/{path:path}",
    methods=["GET", "POST"],
    include_in_schema=False,
)
async def actions_proxy(
    request: fastapi.Request, action_urn: str, path: str
) -> fastapi.Response:
    """Proxy requests to the action."""

    spec = _get_action_spec(action_urn)

    # From https://github.com/tiangolo/fastapi/issues/1788#issuecomment-1071222163
    url = f"{spec.base_url}/{path}?{request.url.query}"
    req = _httpx_client.build_request(
        request.method, url, headers=request.headers.raw, content=await request.body()
    )
    res = await _httpx_client.send(req, stream=True)
    return fastapi.responses.StreamingResponse(
        res.aiter_raw(),
        status_code=res.status_code,
        headers=res.headers,
        background=starlette.background.BackgroundTask(res.aclose),
    )
