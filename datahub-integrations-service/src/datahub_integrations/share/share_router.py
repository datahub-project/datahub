from __future__ import annotations

import uuid
from typing import Optional

import anyio
import fastapi
from datahub.metadata._schema_classes import ShareConfigClass, ShareResultStateClass
from fastapi import BackgroundTasks
from loguru import logger

from datahub_integrations.app import graph
from datahub_integrations.share.api import (
    ExecuteShareResult,
    ExecuteUnshareResult,
    LineageDirection,
)
from datahub_integrations.share.share_agent import ShareAgent

router = fastapi.APIRouter()


_ACTOR_URN = "urn:li:corpuser:__integrations"


def get_or_create_share_agent(share_connection_urn: str) -> ShareAgent:
    # TODO: Maintain a share agent cache globally, and only reload the connection info when it changes.
    return ShareAgent(source_graph=graph, share_connection_urn=share_connection_urn)


@router.post("/execute_share")
def execute_share(
    background_tasks: BackgroundTasks,
    share_connection_urn: str,
    entity_urn: str,
    sharer_urn: str,
    lineage_direction: Optional[LineageDirection] = None,
) -> ExecuteShareResult:
    """Execute a share for a given entity.

    This is a one-time sync action.

    If lineage_direction is specified, then we will share all entities downstream/upstream of the given entity.
    """
    logger.info(
        f"Executing share for entity {entity_urn} to {share_connection_urn} lineage direction {lineage_direction} by user {sharer_urn}"
    )

    share_agent = get_or_create_share_agent(share_connection_urn=share_connection_urn)

    if not lineage_direction:
        # If lineage direction is not provided, we need to determine the lineage direction from the share aspect.
        # This only applies to re-shares. If this is a new share, nothing is impacted.
        lineage_direction = share_agent.get_lineage_direction_from_share_aspect(
            entity_urn
        )
        if lineage_direction:
            logger.debug(
                f"Lineage direction set to {lineage_direction} from the share aspect as it was not provided explicitly."
            )
        else:
            logger.debug(
                "Lineage direction is set to non (only the current asset will be shared) as lineage direction was not provided, not determined from the share aspect, or no share aspect exists."
            )
    # generate a guid for the share request
    share_request_id = str(uuid.uuid4())
    share_agent.emit_share_result(
        root_entity_urn=entity_urn,
        shared_urn=entity_urn,
        sharer_urn=sharer_urn,
        status=ShareResultStateClass.RUNNING,
        share_config=(
            ShareConfigClass(
                enableUpstreamLineage=lineage_direction
                in [LineageDirection.UPSTREAM, LineageDirection.BOTH],
                enableDownstreamLineage=lineage_direction
                in [LineageDirection.DOWNSTREAM, LineageDirection.BOTH],
            )
            if lineage_direction
            else None
        ),
        share_request_id=share_request_id,
    )
    background_tasks.add_task(
        anyio.to_thread.run_sync,
        share_agent.share,
        entity_urn,
        sharer_urn,
        lineage_direction,
        share_request_id,
    )

    return ExecuteShareResult(status="success", entities_shared=[entity_urn])


@router.post("/execute_unshare")
def execute_unshare(
    background_tasks: BackgroundTasks,
    share_connection_urn: str,
    entity_urn: str,
    lineage_direction: LineageDirection | None = None,
) -> ExecuteUnshareResult:
    """Execute an unshare for a given entity.

    This is a one-time sync action.

    If lineage_direction is specified, then we will unshare all entities downstream/upstream of the given entity.

    """
    logger.info(
        f"Executing unshare for entity {entity_urn} to {share_connection_urn} lineage direction: {lineage_direction}"
    )

    share_agent: ShareAgent = get_or_create_share_agent(share_connection_urn)
    share_agent.unshare_status_update(entity_urn, ShareResultStateClass.RUNNING)

    background_tasks.add_task(
        anyio.to_thread.run_sync, share_agent.unshare, entity_urn, lineage_direction
    )
    return ExecuteUnshareResult(status="success", entities_unshared=[entity_urn])
