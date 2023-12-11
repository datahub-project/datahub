from tempfile import mktemp
from typing import Annotated, List

import fastapi
from datahub.configuration.config_loader import load_config_file
from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.pipeline.pipeline_manager import PipelineManager
from fastapi import Body, HTTPException, status
from loguru import logger

ACTIONS_ROUTE = "/actions"
actions_router = fastapi.APIRouter()


pipeline_manager = PipelineManager()


@actions_router.post("/register")
def register_action(
    action_urn: Annotated[str, Body()], action_config: Annotated[str, Body()]
) -> None:
    """Register an action."""

    with open(mktemp(suffix=".yaml"), "w") as f:
        f.write(action_config)
        f.flush()
        logger.info(f"Action config: {action_config}")
        config = load_config_file(f.name)
        pipeline: Pipeline = Pipeline.create(config)
        logger.info(f"Registering action {action_urn}.")
        try:
            pipeline_manager.start_pipeline(action_urn, pipeline)
        except Exception as e:
            logger.error(f"Failed to register action {action_urn}: {e}")
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e)) from e


@actions_router.post("/stop")
def stop_action(action_urn: str) -> str:
    try:
        pipeline_manager.stop_pipeline(action_urn)
        return "OK"
    except Exception as e:
        logger.error(f"Failed to stop action {action_urn}: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e)) from e


@actions_router.get("/list")
def list_running_actions() -> List[str]:
    """Get all running actions."""

    logger.info("Listing all actions.")
    return list(pipeline_manager.pipeline_registry.keys())


@actions_router.get("/status")
def action_status(action_urn: str) -> str:
    """Get status of an action."""

    logger.info(f"Getting status of action {action_urn}.")
    if action_urn in pipeline_manager.pipeline_registry:
        return "RUNNING"
    else:
        return "MISSING"
