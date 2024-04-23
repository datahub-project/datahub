from __future__ import annotations

import json

import datahub.metadata.schema_classes as models
import fastapi
import pydantic
from datahub.configuration.common import ConnectionModel
from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns
from loguru import logger

from datahub_integrations.app import graph
from datahub_integrations.graphql.connection import get_connection_json
from datahub_integrations.share.share_settings import SHARED_ASPECTS

router = fastapi.APIRouter()


# TODO: Maintain a connection cache globally, and only reload the connection info when it changes.

_ACTOR_URN = "urn:li:corpuser:__integrations"


class ShareConfig(ConnectionModel):
    connection: DatahubClientConfig

    # Eventually we might add "config" fields here too. For now, it's
    # just the GMS connection info.


def determine_entities_to_sync(graph: DataHubGraph, root_entity: str) -> set[str]:
    # Currently, the only supported recursive type is the container aspect.

    # Note that we could optimize the perf of this by considering the browsePathV2 aspect,
    # which would allow us to skip the recursion. However, I've written it in a way that
    # will work for arbitrary entity types once we want that.

    entities_to_sync = set()

    def _recursive_check_entity(entity_urn: str) -> None:
        if entity_urn in entities_to_sync:
            return
        entities_to_sync.add(entity_urn)

        container_aspect = graph.get_aspect(entity_urn, models.ContainerClass)
        if container_aspect:
            for nested_urn in list_urns(container_aspect):
                _recursive_check_entity(nested_urn)

    _recursive_check_entity(root_entity)
    return entities_to_sync


def update_share_aspect(
    existing_share_aspect: models.ShareClass | None,
    destination_urn: str,
    implicit_share_entity: str | None,
) -> models.ShareClass:
    # Copy the existing share aspect or init an empty one.
    share_aspect = (
        models.ShareClass.from_obj(existing_share_aspect.to_obj())
        if existing_share_aspect
        else models.ShareClass(lastShareResults=[])
    )

    current_audit_stamp = models.AuditStampClass(
        time=get_sys_time(),
        actor=_ACTOR_URN,
    )

    share_result: models.ShareResultClass | None = None
    for res in share_aspect.lastShareResults:
        if res.destination == destination_urn:
            share_result = res

            if not implicit_share_entity:
                # If this was previously implicitly shared and now we're explicitly sharing it,
                # then we need to clear the implicit share entity.
                share_result.implicitShareEntity = None

            break
    else:
        # No entry for this destination yet, so we need to add one.
        share_result = models.ShareResultClass(
            destination=destination_urn,
            created=current_audit_stamp,
            lastAttempt=current_audit_stamp,
            status=models.ShareResultStateClass.SUCCESS,
            implicitShareEntity=implicit_share_entity,
        )
        share_aspect.lastShareResults.append(share_result)

    # Update the share result.
    share_result.status = models.ShareResultStateClass.SUCCESS
    share_result.lastAttempt = current_audit_stamp
    share_result.lastSuccess = current_audit_stamp
    share_result.message = None

    return share_aspect


def share_entity(
    source_graph: DataHubGraph,
    destination_urn: str,
    destination_graph: DataHubGraph,
    shared_urn: str,
    root_entity_urn: str,
) -> None:
    # TODO: This does not work for timeseries aspects.

    raw_entity = source_graph.get_entity_raw(shared_urn)
    raw_entity_aspects = raw_entity.get("aspects", {})

    shareable_aspects = {
        aspect_name: aspect["value"]
        for aspect_name, aspect in raw_entity_aspects.items()
        if aspect_name in SHARED_ASPECTS
    }

    logger.info(
        f"For {shared_urn}, sharing {len(shareable_aspects)} aspects: "
        f"{list(shareable_aspects.keys())}"
    )

    destination_mcps = [
        models.MetadataChangeProposalClass(
            entityType=guess_entity_type(shared_urn),
            changeType=models.ChangeTypeClass.UPSERT,
            entityUrn=shared_urn,
            aspectName=aspect_name,
            aspect=models.GenericAspectClass(
                value=json.dumps(aspect).encode(),
                contentType=JSON_CONTENT_TYPE,
            ),
        )
        for aspect_name, aspect in shareable_aspects.items()
    ]

    for mcp in destination_mcps:
        # TODO: Set systemMetadata for the MCPs.
        # TODO: We also need to mark these entities as shared in the destination.
        destination_graph.emit(mcp)

    # Finally, update the source_graph's share aspect.
    existing_share_aspect: models.ShareClass = source_graph.get_aspect(
        # Technically this reads from the source graph again, but it
        # lets us leverage the deserialization logic in the graph.
        shared_urn,
        models.ShareClass,
    ) or models.ShareClass(lastShareResults=[])

    share_aspect = update_share_aspect(
        existing_share_aspect=existing_share_aspect,
        destination_urn=destination_urn,
        implicit_share_entity=root_entity_urn
        if root_entity_urn != shared_urn
        else None,
    )

    source_graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=shared_urn,
            aspect=share_aspect,
        )
    )


def unshare_entity(
    source_graph: DataHubGraph,
    destination_urn: str,
    destination_graph: DataHubGraph,
    unshared_urn: str,
) -> ExecuteUnshareResult:
    # TODO: This does not work for timeseries aspects.

    # Check if unshared_urn is in destination
    urn_in_dest = destination_graph.exists(unshared_urn)

    if not urn_in_dest:
        logger.info(
            f"Cannot unshare urn {unshared_urn} because it has not been shared to destination {destination_urn}."
        )
        unshare_result = ExecuteUnshareResult(
            status="ok",
            entities_unshared=[],
        )
    else:
        # Send a soft delete to the destination
        destination_graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=unshared_urn, aspect=models.StatusClass(removed=True)
            )
        )

        # Find and remove destination_urn from share aspect in source system
        existing_share_aspect = source_graph.get_aspect(unshared_urn, models.ShareClass)
        if existing_share_aspect:
            updated_share_results = models.ShareClass(lastShareResults=[])

            for share_result in existing_share_aspect.lastShareResults:
                if share_result.destination != destination_urn:
                    updated_share_results.lastShareResults.append(share_result)

            source_graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=unshared_urn,
                    aspect=updated_share_results,
                )
            )

        unshare_result = ExecuteUnshareResult(
            status="ok",
            entities_unshared=[unshared_urn],
        )
    return unshare_result


class ExecuteShareResult(pydantic.BaseModel):
    status: str
    entities_shared: list[str]


@router.post("/execute_share")
def execute_share(share_connection_urn: str, entity_urn: str) -> ExecuteShareResult:
    """Execute a share for a given entity.

    This is a one-time sync action.
    """

    share_config_raw = get_connection_json(graph, share_connection_urn)
    share_config = ShareConfig.parse_obj(share_config_raw)
    destination_graph = DataHubGraph(share_config.connection)
    logger.debug(f"Using destination graph: {destination_graph!r}")

    # First, we need to build the full set of entities to sync.
    entities_to_sync = list(determine_entities_to_sync(graph, entity_urn))
    logger.info(f"Going to sync {len(entities_to_sync)} entities: {entities_to_sync}")

    # Then, we sync each entity.
    for shared_urn in entities_to_sync:
        share_entity(
            source_graph=graph,
            destination_urn=share_connection_urn,
            destination_graph=destination_graph,
            shared_urn=shared_urn,
            root_entity_urn=entity_urn,
        )

    return ExecuteShareResult(
        status="ok",
        entities_shared=entities_to_sync,
    )


class ExecuteUnshareResult(pydantic.BaseModel):
    status: str
    entities_unshared: list[str]


@router.post("/execute_unshare")
def execute_unshare(share_connection_urn: str, entity_urn: str) -> ExecuteUnshareResult:
    """Execute an unshare for a given entity.

    This is a one-time sync action.
    """

    share_config_raw = get_connection_json(graph, share_connection_urn)
    share_config = ShareConfig.parse_obj(share_config_raw)
    destination_graph = DataHubGraph(share_config.connection)
    logger.debug(f"Using destination graph: {destination_graph!r}")

    unshare_result = unshare_entity(
        source_graph=graph,
        destination_urn=share_connection_urn,
        destination_graph=destination_graph,
        unshared_urn=entity_urn,
    )

    return unshare_result
