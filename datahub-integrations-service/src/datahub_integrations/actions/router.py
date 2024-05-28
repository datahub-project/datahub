import asyncio
import contextlib
import json
import os
import pathlib
from datetime import datetime
from typing import Any, AsyncIterator, Callable

import anyio
import fastapi
import httpx
import reactpy
import starlette
import starlette.background
from fastapi import HTTPException, status
from loguru import logger
from reactpy import html

from datahub_integrations.actions.actions_manager import (
    ActionRun,
    ActionsManager,
    LiveActionSpec,
)
from datahub_integrations.app import DATAHUB_SERVER, graph

ACTIONS_ROUTE = "/actions"
actions_router = fastapi.APIRouter()

actions_gql = (pathlib.Path(__file__).parent / "actions.gql").read_text()


base_action_config = {
    "name": "hello_world",
    "source": {
        "type": "kafka",
        "config": {
            "topic_routes": {
                "mcl": "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1}",
                "pe": "${PLATFORM_EVENT_TOPIC_NAME:-PlatformEvent_v1}",
            },
            "connection": {
                "bootstrap": "${KAFKA_BOOTSTRAP_SERVER:-broker:29092}",
                "schema_registry_url": "${SCHEMA_REGISTRY_URL:-http://datahub-gms:8080/schema-registry/api/}",
                "consumer_config": {
                    "auto.offset.reset": "${KAFKA_AUTO_OFFSET_POLICY:-latest}",
                    "security.protocol": "${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}",
                    **(
                        {
                            "sasl.mechanism": "${KAFKA_PROPERTIES_SASL_MECHANISM:-PLAIN}",
                            "sasl.username": "${KAFKA_PROPERTIES_SASL_USERNAME:-}",
                            "sasl.password": "${KAFKA_PROPERTIES_SASL_PASSWORD:-}",
                        }
                        if os.environ.get("KAFKA_PROPERTIES_SASL_MECHANISM")
                        else {}
                    ),
                    # Reset these to their default values, as per
                    # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
                    # Undoing the changes in datahub-actions:
                    # https://github.com/acryldata/datahub-actions/blob/18c118d351346b3721cc3b07ce5dac0b58fa8b23/datahub-actions/src/datahub_actions/plugin/source/kafka/kafka_event_source.py#L134
                    "session.timeout.ms": 45 * 1000,
                    "max.poll.interval.ms": 300 * 1000,
                },
            },
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

        try:
            all_actions = graph.execute_graphql(
                query=actions_gql, operation_name="listActions"
            )
            logger.debug(f"Got actions: {all_actions}")

            try:
                for action in all_actions["listActionPipelines"]["actionPipelines"]:
                    action_urn = action["urn"]
                    action_details = action["details"]

                    logger.info(f"Starting action {action_urn}.")
                    await start_or_restart_action(
                        action_urn, action_details, throw=False
                    )

                yield

            finally:
                logger.info("Stopping all running actions.")
                await pipeline_manager.stop_all()

                logger.info("All actions stopped.")
        except Exception as e:
            logger.exception(
                f"Failed to start actions: {e}. Continuing with no actions."
            )
            yield


@actions_router.get("/")
def list_running_actions() -> list[str]:
    """Get all running actions."""

    return list(pipeline_manager.pipelines.keys())


async def start_or_restart_action(
    action_urn: str, action_details: dict, throw: bool = True
) -> None:
    if pipeline_manager.is_running(action_urn):
        logger.info(f"Stopping action {action_urn}.")
        await pipeline_manager.stop_pipeline(action_urn)

    try:
        recipe = get_config_from_details(action_urn, action_details)

        logger.debug(f"Starting action {action_urn} with recipe: {recipe}")
        await pipeline_manager.start_pipeline(action_urn, recipe)
    except Exception as e:
        if throw:
            logger.error(f"Failed to register action {action_urn}: {e}")
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e)) from e
        else:
            pipeline_manager.report_dead_pipeline(action_urn, action_details, e)


def get_config_from_details(action_urn: str, action_details: dict) -> dict:
    action_details_recipe = json.loads(action_details["config"]["recipe"])
    # TODO respect version and other details

    recipe = {
        **base_action_config,
        "name": action_urn,
        **action_details_recipe,
    }

    return recipe


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


def _get_action_spec(action_urn: str) -> LiveActionSpec:
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
    logger.info(f"Stopping action {action_urn}.")

    try:
        await pipeline_manager.stop_pipeline(action_urn)
        return "OK"
    except Exception as e:
        logger.error(f"Failed to stop action {action_urn}: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e)) from e


@actions_router.post("/{action_urn}/rollback")
async def rollback_action(action_urn: str) -> str:
    """
    Rollback (and stop) an action.

    """
    try:
        action_spec = _get_action_spec(action_urn)
        config = action_spec.action_run.unresolved_config
    except HTTPException:
        updated_details = graph.execute_graphql(
            actions_gql, operation_name="getAction", variables={"urn": action_urn}
        )["actionPipeline"]["details"]
        config = get_config_from_details(action_urn, updated_details)

    try:
        await pipeline_manager.rollback_pipeline(action_urn, config=config)
        return "OK"
    except Exception as e:
        logger.error(f"Failed to stop action {action_urn}: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e)) from e


@actions_router.get("/{action_urn}/rollbackstats")
async def rollback_stats(action_urn: str) -> dict:
    """
    Last rollback stats for an action.

    """
    try:
        rollback_results = pipeline_manager.rolledback_pipelines.get(action_urn)
        if not rollback_results:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No rollback stats found.")
        else:
            rollback_stats = {
                "action_urn": action_urn,
                "status": rollback_results.status,
                "started_at": datetime.strftime(
                    rollback_results.started_at, "%Y-%m-%d %H:%M:%S"
                ),
                "logs": rollback_results.action_run.logs.get_logs(),
                "ended_at": (
                    datetime.strftime(rollback_results.ended_at, "%Y-%m-%d %H:%M:%S")
                    if rollback_results.ended_at
                    else None
                ),
            }
            return rollback_stats
    except Exception as e:
        logger.error(f"Failed to get rollback stats for action {action_urn}: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e)) from e


@actions_router.get("/{action_urn}/live_logs")
async def action_logs(action_urn: str) -> fastapi.Response:
    """Get the most recent logs from the action."""

    spec = _get_action_spec(action_urn)

    return fastapi.responses.PlainTextResponse(content=spec.action_run.logs.get_logs())


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


@reactpy.component
def ActionInfo(spec: ActionRun | LiveActionSpec) -> reactpy.types.VdomDict:
    async def handle_reload(event: Any = None) -> None:
        await reload_action(spec.urn)

    async def handle_stop(event: Any = None) -> None:
        await stop_action(spec.urn)

    stats, set_stats = reactpy.use_state("Loading...")

    async def update_stats(event: Any = None) -> None:
        if not isinstance(spec, LiveActionSpec):
            set_stats("Action is not running.")
            return

        try:
            stat_res = await action_stats(spec.urn)
            logger.debug(f"Got stats in router: {stat_res}")
            set_stats(json.dumps(stat_res, indent=2))
        except Exception as e:
            set_stats(f"Failed to get stats: {e}")

    # TODO: Set a timer to update stats every n seconds.
    reactpy.use_effect(update_stats, dependencies=[spec.urn])

    button_css = {"className": "ui button"}
    pre_css = {"style": {"maxHeight": "30rem", "overflow": "auto"}}

    return html.div(
        {"className": "ui segment", "style": {"margin": "1rem 0"}},
        html.h2(html.code(spec.urn)),
        (
            html.div(
                html.span("Stats:"),
                html.pre(pre_css, stats),
            )
            if isinstance(spec, LiveActionSpec)
            else html._()
        ),
        html.div(
            html.span("Logs:"),
            html.pre(pre_css, spec.action_run.logs.get_logs()),
        ),
        (
            html.button(
                {"onClick": update_stats, **button_css},
                "Reload Stats",
            )
            if isinstance(spec, LiveActionSpec)
            else html._()
        ),
        html.button(
            {"onClick": handle_reload, **button_css},
            "Restart",
        ),
        (
            html.button(
                {"onClick": handle_stop, **button_css},
                "Stop",
            )
            if isinstance(spec, LiveActionSpec)
            else html._()
        ),
    )


@reactpy.component
def ActionsAdminUi() -> reactpy.types.VdomDict:
    # TODO Add reactpy-router to create multiple pages.

    last_updated, set_last_updated = reactpy.use_state(datetime.now().isoformat())
    inner_pipeline_manager, set_inner_pipeline_manager = reactpy.use_state(
        pipeline_manager
    )

    async def pipeline_updater() -> None:
        while True:
            set_inner_pipeline_manager(pipeline_manager)
            set_last_updated(datetime.now().isoformat())
            await anyio.sleep(2)

    @reactpy.use_effect(dependencies=[])
    def update_pipeline_effect() -> Callable:
        # Via https://github.com/reactive-python/reactpy/discussions/966
        task = asyncio.create_task(pipeline_updater())
        return task.cancel

    # TODO: Show pipelines that have died/crashed, including their logs.
    return html.div(
        {"style": {"margin": "1rem 2rem 0"}},
        html.span(f"Last updated: {last_updated}"),
        html.section(
            html.h1("Active Actions"),
            *[
                ActionInfo(spec, key=spec.urn)
                for spec in inner_pipeline_manager.pipelines.values()
            ],
        ),
        html.section(
            html.h1("Stopped Actions"),
            html.p(
                "These actions have stopped running. This includes the last failure of any action that has since been restarted."
            ),
            *[
                ActionInfo(spec, key=spec.urn)
                for spec in inner_pipeline_manager.dead_pipelines.values()
            ],
        ),
    )
