from __future__ import annotations

import json
import logging
import time
from typing import Callable, Dict, List, Optional, Set, Union

import datahub.metadata.schema_classes as models
from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mce_builder import (
    get_sys_time,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import ShareConfigClass, ShareResultStateClass
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns
from loguru import logger

from datahub_integrations.graphql.connection import get_connection_json
from datahub_integrations.share.api import (
    ExecuteShareResult,
    ExecuteUnshareResult,
    LineageDirection,
    ShareConfig,
)
from datahub_integrations.share.share_settings import (
    ACTOR_URN,
    MAX_ENTITIES_PER_SHARE,
    REPORTING_HEARTBEAT_INTERVAL,
    RESTRICTED_SHARED_ASPECTS,
    SHARED_ASPECTS,
    SKIP_CACHE_ON_LINEAGE_QUERY,
)

GET_LINEAGE_GQL = """
query GetDownstreams($input:ScrollAcrossLineageInput!) {
  scrollAcrossLineage(input: $input) {
    searchResults {
      entity {
        urn
        type
      }
    }
    nextScrollId
    count
    total
  }
}
"""


class ShareAgent:
    def __init__(
        self,
        source_graph: DataHubGraph,
        share_connection_urn: str,
        destination_graph: Optional[DataHubGraph] = None,
    ):
        self.source_graph = source_graph
        self.source_share_connection_urn = share_connection_urn
        self.source_platform_instance = ShareAgent.get_platform_instance(
            self.source_graph
        )
        if not destination_graph:
            self.destination_graph = ShareAgent.create_destination_graph(
                self.source_graph, share_connection_urn
            )
        else:
            self.destination_graph = destination_graph

    @staticmethod
    def get_platform_instance(graph: DataHubGraph) -> str:
        if graph.server_id and graph.server_id != "missing":
            platform_id = graph.server_id
        else:
            platform_id = graph.get_config().get("baseUrl") or ""

        return make_dataplatform_instance_urn("acryl", platform_id)

    @staticmethod
    def create_destination_graph(
        graph: DataHubGraph, connection_urn: str
    ) -> DataHubGraph:
        share_config_raw = get_connection_json(graph, connection_urn)
        share_config = ShareConfig.model_validate(share_config_raw)
        destination_graph = DataHubGraph(share_config.connection)
        logger.debug(f"Using destination graph: {destination_graph!r}")
        return destination_graph

    def determine_entities_to_sync(self, root_entity: str) -> set[str]:
        # Currently, the only supported recursive type is the container aspect.

        # Note that we could optimize the perf of this by considering the browsePathV2 aspect,
        # which would allow us to skip the recursion. However, I've written it in a way that
        # will work for arbitrary entity types once we want that.
        entities_to_sync = set()
        logger.info(f"Determining entities to sync for root entity: {root_entity}")

        def _recursive_check_entity(entity_urn: str) -> None:
            if entity_urn in entities_to_sync:
                logger.debug(f"Already checked {entity_urn}")
                return

            entities_to_sync.add(entity_urn)
            container_aspect = self.source_graph.get_aspect(
                entity_urn, models.ContainerClass
            )
            if container_aspect:
                logger.info(
                    f"Found container aspect for {entity_urn} -> {container_aspect}"
                )
                for nested_urn in list_urns(container_aspect):
                    _recursive_check_entity(nested_urn)

            structured_properties = self.source_graph.get_aspect(
                entity_urn, models.StructuredPropertiesClass
            )
            if structured_properties:
                for nested_urn in list_urns(structured_properties):
                    _recursive_check_entity(nested_urn)

        _recursive_check_entity(root_entity)
        return entities_to_sync

    def get_entities_across_lineage(
        self, entity_urn: str, lineage_direction: LineageDirection, max_entities: int
    ) -> Set[str]:
        entities_to_sync: Set[str] = set()
        variables: Dict = {
            "input": {
                "urn": entity_urn,
                "direction": lineage_direction.value,
            }
        }

        if SKIP_CACHE_ON_LINEAGE_QUERY:
            variables["input"]["searchFlags"] = {
                "skipCache": True,
                "fulltext": False,  # these flags are required, even though we don't use them
                "maxAggValues": 10,  # these flags are required, even though we don't use them
            }

        scroll_id: Optional[str] = None
        while len(entities_to_sync) < max_entities or (max_entities == -1):
            logger.info(
                f"Fetching entities of {lineage_direction} for {entity_urn} with scrollId: {scroll_id}"
            )
            if scroll_id:
                variables["input"]["scrollId"] = scroll_id

            res = self.source_graph.execute_graphql(
                GET_LINEAGE_GQL, variables=variables
            )
            scroll_id = res["scrollAcrossLineage"]["nextScrollId"]
            for entity in res["scrollAcrossLineage"]["searchResults"]:
                entities = self.determine_entities_to_sync(entity["entity"]["urn"])
                entities_to_sync = entities_to_sync.union(entities)

            if not scroll_id:
                break

        logger.info(
            f"Found {len(entities_to_sync)} entities of type {lineage_direction} for {entity_urn}. Entities: {entities_to_sync}"
        )
        if len(entities_to_sync) >= max_entities:
            logger.warning(
                f"Reached max entities to sync for {entity_urn} with {len(entities_to_sync)} entities with lineage direction {lineage_direction}. Will share only {max_entities} entities."
            )

        return entities_to_sync

    def update_share_aspect(
        self,
        shared_urn: str,
        existing_share_aspect: models.ShareClass | None,
        source_share_connection_urn: str,
        sharer_urn: str,
        implicit_share_entity: str | None,
        status: Union[str, ShareResultStateClass] = ShareResultStateClass.SUCCESS,
        share_config: Optional[ShareConfigClass] = None,
        share_request_id: Optional[str] = None,
    ) -> models.ShareClass:
        # Copy the existing share aspect or init an empty one.
        share_aspect = (
            models.ShareClass.from_obj(existing_share_aspect.to_obj())
            if existing_share_aspect
            else models.ShareClass(lastShareResults=[])
        )

        current_audit_stamp = models.AuditStampClass(
            time=get_sys_time(),
            actor=sharer_urn,
        )

        logger.debug(
            f"Updating share aspect for {shared_urn} for {source_share_connection_urn} with implicit share entity: {implicit_share_entity}"
        )
        share_result: models.ShareResultClass
        share_results: List[models.ShareResultClass] = []
        share_to_add: Optional[models.ShareResultClass] = None
        for res in share_aspect.lastShareResults:
            share_result = res
            if res.destination == source_share_connection_urn:
                if implicit_share_entity == share_result.implicitShareEntity:
                    # If this was previously implicitly shared and now we're implicitly sharing it again,
                    # then we need replace the old share.
                    logger.debug(
                        f"Entity was already implicitly shared with {source_share_connection_urn}. Replacing."
                    )
                    share_to_add = share_result
                    break
                else:
                    share_results.append(share_result)
            else:
                share_results.append(share_result)

        if not share_to_add:
            # No entry for this destination, implicity share entity yet, so we need to add one.
            share_to_add = models.ShareResultClass(
                destination=source_share_connection_urn,
                created=current_audit_stamp,
                lastAttempt=current_audit_stamp,
                status=status,
                implicitShareEntity=implicit_share_entity,
                shareConfig=share_config,
                lastAttemptRequestId=share_request_id,
            )

        # Update the share result.
        share_to_add.status = status
        share_to_add.lastAttempt = (
            share_to_add.lastAttempt or current_audit_stamp
        )  # use the current last attempt id if it exists
        if status == ShareResultStateClass.SUCCESS:
            share_to_add.lastSuccess = current_audit_stamp
        if status == ShareResultStateClass.PARTIAL_SUCCESS:
            share_to_add.lastSuccess = current_audit_stamp
            # message will be presented to the end user
            share_to_add.message = "Successfully shared asset with some warnings"
        share_to_add.message = None
        share_to_add.shareConfig = share_config
        share_to_add.lastAttemptRequestId = share_request_id
        share_to_add.statusLastUpdated = current_audit_stamp.time

        share_results.append(share_to_add)
        logging.debug(
            f"Adding share result for {source_share_connection_urn} with implict share {implicit_share_entity}"
        )

        share_aspect.lastShareResults = share_results

        return share_aspect

    def update_unshare_aspect(
        self,
        unshared_urn: str,
        existing_share_aspect: models.ShareClass,
        source_share_connection_urn: str,
        status: Union[str, ShareResultStateClass],
        share_request_id: Optional[str] = None,
    ) -> Optional[models.ShareClass]:
        share_aspect = models.ShareClass.from_obj(existing_share_aspect.to_obj())

        unshare_to_add: Optional[models.ShareResultClass] = None

        unshares: List[models.ShareResultClass] = []

        if status == ShareResultStateClass.RUNNING:
            # First find the shareResult we want to unshare
            for share_result in share_aspect.lastShareResults:
                if (
                    share_result.destination == self.source_share_connection_urn
                    and share_result.implicitShareEntity is None
                ):
                    unshare_to_add = models.ShareResultClass.from_obj(
                        share_result.to_obj()
                    )
                    break

            if not unshare_to_add:
                logger.warning(
                    f"Entity {unshared_urn} does not have a share aspect for the source connection. Maybe it was already unshared or it was implicit share?"
                )
                return None

        if share_aspect.lastUnshareResults:
            for share_result in share_aspect.lastUnshareResults:
                if share_result.destination != self.source_share_connection_urn:
                    unshares.append(share_result)
                else:
                    # If we already have an unshare then we should update that or use the newly created unshare if we have it
                    unshare_to_add = (
                        share_result if not unshare_to_add else unshare_to_add
                    )

            if not unshare_to_add:
                logger.warning(
                    f"Entity {unshared_urn} does not have a share aspect for the source connection. Maybe it was already unshared?"
                )
                return None

        if unshare_to_add:
            unshare_to_add.lastAttempt.time = get_sys_time()
            unshare_to_add.status = status
            unshare_to_add.statusLastUpdated = get_sys_time()
            unshares.append(unshare_to_add)

        share_aspect.lastUnshareResults = unshares

        return share_aspect

    def transform_aspect(
        self, aspect: dict, aspect_type: str, restricted: bool
    ) -> dict:
        if not restricted:
            return aspect

        if aspect.get("fineGrainedLineages"):
            aspect["fineGrainedLineages"] = []

        return aspect

    def share_one_entity(
        self,
        shared_urn: str,
        root_entity_urn: str,
        sharer_urn: str,
        restricted: bool = False,
        share_config: Optional[ShareConfigClass] = None,
        system_metadata: Optional[models.SystemMetadataClass] = None,
        update_share_aspect: bool = True,
    ) -> Optional[Callable]:
        # TODO: This does not work for timeseries aspects.
        raw_entity = self.source_graph.get_entity_raw(shared_urn)
        raw_entity_aspects = raw_entity.get("aspects", {})

        allowed_aspects = (
            SHARED_ASPECTS if not restricted else RESTRICTED_SHARED_ASPECTS
        )

        shareable_aspects = {
            aspect_name: aspect["value"]
            for aspect_name, aspect in raw_entity_aspects.items()
            if aspect_name in allowed_aspects
        }

        # status is special - we emit it even if it's not in the allowed_aspects
        # list.
        if "status" not in shareable_aspects:
            shareable_aspects["status"] = {"removed": False}

        # origin is also special - we replace it with our version
        origin_aspect = models.OriginClass(
            type=models.OriginTypeClass.EXTERNAL,
            externalType=None,
            sourceDetails=[
                models.SourceDetailsClass(
                    source=self.source_platform_instance,
                    platform=make_data_platform_urn("acryl"),
                    lastModified=models.AuditStampClass(
                        time=int(time.time() * 1000),
                        actor=sharer_urn,
                        impersonator=ACTOR_URN,
                    ),
                    mechanism=models.SyncMechanismClass.SHARE,
                )
            ],
        ).to_obj()
        shareable_aspects["origin"] = origin_aspect

        logger.debug(
            f"For {shared_urn}, {'restricted ' if restricted else ''}sharing {len(shareable_aspects)} aspects: "
            f"{list(shareable_aspects.keys())}"
        )

        destination_mcps = [
            models.MetadataChangeProposalClass(
                entityType=guess_entity_type(shared_urn),
                changeType=models.ChangeTypeClass.UPSERT,
                entityUrn=shared_urn,
                aspectName=aspect_name,
                aspect=models.GenericAspectClass(
                    value=json.dumps(
                        self.transform_aspect(aspect, aspect_name, restricted)
                    ).encode(),
                    contentType=JSON_CONTENT_TYPE,
                ),
                systemMetadata=system_metadata,
            )
            for aspect_name, aspect in shareable_aspects.items()
        ]

        failure_count = 0
        error_messages = []
        for mcp in destination_mcps:
            # TODO: We also need to mark these entities as shared in the
            # destination.
            try:
                self.destination_graph.emit(mcp)
            except Exception as e:
                logger.error(
                    f"Failed to emit entity {mcp.entityUrn}, aspect {mcp.aspectName} to destination: {e}"
                )
                error_messages.append(f"{mcp.aspectName}: {e}")
                failure_count += 1

        all_aspects_failed = failure_count == len(destination_mcps)

        share_status = ShareResultStateClass.SUCCESS
        if failure_count > 0:
            share_status = (
                ShareResultStateClass.FAILURE
                if all_aspects_failed
                else ShareResultStateClass.PARTIAL_SUCCESS
            )

        share_aspect_emitter_func = lambda: self.emit_share_result(  # noqa: E731
            root_entity_urn,
            shared_urn,
            sharer_urn,
            share_status,
            share_config,
        )
        if update_share_aspect:
            share_aspect_emitter_func()
        else:
            return share_aspect_emitter_func

        return None

    def emit_share_result(
        self,
        root_entity_urn: str,
        shared_urn: str,
        sharer_urn: str,
        status: str = ShareResultStateClass.SUCCESS,
        share_config: Optional[ShareConfigClass] = None,
        share_request_id: Optional[str] = None,
    ) -> None:
        # Finally, update the source_graph's share aspect.
        existing_share_aspect: models.ShareClass = self.source_graph.get_aspect(
            # Technically this reads from the source graph again, but it
            # lets us leverage the deserialization logic in the graph.
            shared_urn,
            models.ShareClass,
        ) or models.ShareClass(lastShareResults=[])
        share_aspect = self.update_share_aspect(
            shared_urn=shared_urn,
            existing_share_aspect=existing_share_aspect,
            source_share_connection_urn=self.source_share_connection_urn,
            sharer_urn=sharer_urn,
            implicit_share_entity=(
                root_entity_urn if root_entity_urn != shared_urn else None
            ),
            share_config=share_config,
            status=status,
            share_request_id=share_request_id,
        )

        self.source_graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=shared_urn,
                aspect=share_aspect,
            )
        )

    def unshare_one_entity(
        self,
        unshared_urn: str,
        implicit_share_entity: str | None = None,
    ) -> str | None:
        # TODO: This does not work for timeseries aspects.

        # Find and remove destination_datahub_urn from share aspect in source system
        existing_share_aspect = self.source_graph.get_aspect(
            unshared_urn, models.ShareClass
        )
        if not existing_share_aspect or not existing_share_aspect.lastShareResults:
            logger.info(
                f"Entity {unshared_urn} does not have a share aspect or no share results."
            )
            return None
        updated_share_results = models.ShareClass(lastShareResults=[])
        destination_reference_left = 0

        # We need to carry over the last unshared results
        updated_share_results.lastUnshareResults = (
            existing_share_aspect.lastUnshareResults
        )

        for share_result in existing_share_aspect.lastShareResults:
            if share_result.destination != self.source_share_connection_urn:
                updated_share_results.lastShareResults.append(share_result)
                continue

            if share_result.destination == self.source_share_connection_urn:
                if not share_result.implicitShareEntity and implicit_share_entity:
                    logger.debug(
                        "Implicit unshare should not unshare an explicitly shared entity"
                    )
                    return None

                # If implicit share entity is provided, then we need to unshare the entity only it doesn't have any more reference.
                if (
                    implicit_share_entity
                    and share_result.implicitShareEntity != implicit_share_entity
                ):
                    destination_reference_left += 1
                    logger.debug(
                        f"Destination reference left for {unshared_urn} in {self.source_share_connection_urn} is {destination_reference_left}"
                    )
                    updated_share_results.lastShareResults.append(share_result)
                    continue

        if destination_reference_left == 0:
            logger.debug(
                f"Destination reference left for {unshared_urn} in {self.source_share_connection_urn} is 0. Soft-deleting the entity."
            )
            # Send a soft delete to the destination
            # Check if unshared_urn is in destination
            urn_in_dest = self.destination_graph.exists(unshared_urn)
            if urn_in_dest:
                self.destination_graph.emit(
                    MetadataChangeProposalWrapper(
                        entityUrn=unshared_urn,
                        aspect=models.StatusClass(removed=True),
                    )
                )
            else:
                logger.info(
                    f"Not soft-deleting {unshared_urn} because it has not been shared/does not exists in destination {self.source_share_connection_urn}."
                )
                return None
        else:
            logger.info(
                "Not soft-deleting the entity as it still has reference to the same destination."
            )

        logger.debug(f"{updated_share_results} for {unshared_urn}")
        self.source_graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=unshared_urn,
                aspect=updated_share_results,
            )
        )

        return unshared_urn

    def share(
        self,
        entity_urn: str,
        sharer_urn: str,
        lineage_direction: Optional[LineageDirection] = None,
        share_request_id: Optional[str] = None,
    ) -> ExecuteShareResult:
        # Record the start time of this operation
        last_report_time = time.time()

        system_metadata = models.SystemMetadataClass(runId=share_request_id)

        logger.debug(f"Using destination graph: {self.destination_graph!r}")

        # First, we need to build the full set of entities to sync.
        entities_to_sync = self.determine_entities_to_sync(entity_urn)
        if lineage_direction:
            if lineage_direction == LineageDirection.BOTH:
                lineage_direction_list = [
                    LineageDirection.UPSTREAM,
                    LineageDirection.DOWNSTREAM,
                ]
            else:
                lineage_direction_list = [lineage_direction]

            for direction in lineage_direction_list:
                lineage_entities = self.get_entities_across_lineage(
                    entity_urn,
                    lineage_direction=direction,
                    max_entities=MAX_ENTITIES_PER_SHARE,
                )
                entities_to_sync = entities_to_sync.union(lineage_entities)
        logger.info(
            f"Going to sync {len(entities_to_sync)} entities: {entities_to_sync}"
        )

        share_config = ShareConfigClass(
            enableDownstreamLineage=lineage_direction
            in [
                LineageDirection.DOWNSTREAM,
                LineageDirection.BOTH,
            ],
            enableUpstreamLineage=lineage_direction
            in [LineageDirection.UPSTREAM, LineageDirection.BOTH],
        )

        count = 0
        # We first remove the entity_urn from the list to ensure that it's shared last.
        # The shared entity should have shared an share with IN_PROGRESS status earlier

        entities_to_sync.remove(entity_urn)
        # We want structured properties to send first to do this we create a simple key function which makes sure structured properties are sent first
        structured_properties = sorted(
            list(
                filter(
                    lambda x: x.startswith("urn:li:structuredProperty:"),
                    entities_to_sync,
                )
            )
        )
        entities_to_sync_list = (
            structured_properties
            + sorted(list(entities_to_sync.difference(structured_properties)))
            + [entity_urn]
        )
        root_entity_status_emitter = None
        # Then, we sync each entity.
        for shared_urn in entities_to_sync_list:
            ownership = self.source_graph.get_ownership(shared_urn)
            restricted = True
            if ownership:
                for owner in ownership.owners:
                    logger.debug(f"Owner: {owner.owner}")
                    if owner.owner == sharer_urn:
                        restricted = False
                        break
            else:
                # If there's no ownership, we assume it's not restricted.
                restricted = False

            emitter_func = self.share_one_entity(
                shared_urn=shared_urn,
                root_entity_urn=entity_urn,
                sharer_urn=sharer_urn,
                restricted=restricted,
                share_config=share_config,
                system_metadata=system_metadata,
                update_share_aspect=(
                    False if shared_urn == entity_urn else True
                ),  # We update the share aspect for the entity_urn at the end.
            )
            if shared_urn == entity_urn and emitter_func:
                root_entity_status_emitter = emitter_func

            count += 1
            if count % 10 == 0:
                # Only check for reporting every 10 entities.
                if time.time() - last_report_time > REPORTING_HEARTBEAT_INTERVAL:
                    logger.info(
                        f"Shared {count} out of {len(entities_to_sync)} entities."
                    )
                    self.emit_share_result(
                        entity_urn,
                        entity_urn,
                        sharer_urn,
                        ShareResultStateClass.RUNNING,
                        share_config=share_config,
                        share_request_id=share_request_id,
                    )
                    last_report_time = time.time()

        # Finally, update the share aspect for the root entity.
        if root_entity_status_emitter:
            root_entity_status_emitter()

        logger.info(f"Shared {len(entities_to_sync_list)} entities.")
        result = ExecuteShareResult(
            status="ok",
            entities_shared=entities_to_sync_list,
        )

        logger.debug(f"Result: {result}")
        return result

    def get_lineage_direction_from_share_aspect(
        self, entity_urn: str
    ) -> Optional[LineageDirection]:
        share_aspect = self.source_graph.get_aspect(entity_urn, models.ShareClass)
        if not share_aspect or not share_aspect.lastShareResults:
            logger.info(f"Entity {entity_urn} does not have a share aspect.")
            return None
        if share_aspect.lastShareResults:
            logger.debug(f"Share aspect: {share_aspect}")
            this_share_result = [
                x
                for x in share_aspect.lastShareResults
                if x.destination == self.source_share_connection_urn
                and x.implicitShareEntity is None
                and x.lastAttempt is not None
            ]
            sorted(this_share_result, key=lambda x: x.lastAttempt.time, reverse=True)
            if this_share_result:
                this_share = this_share_result[0]
                if this_share.shareConfig:
                    if (
                        this_share.shareConfig.enableDownstreamLineage
                        and this_share.shareConfig.enableUpstreamLineage
                    ):
                        return LineageDirection.BOTH
                    elif this_share.shareConfig.enableUpstreamLineage:
                        return LineageDirection.UPSTREAM
                    elif this_share.shareConfig.enableDownstreamLineage:
                        return LineageDirection.DOWNSTREAM
        return None

    def unshare_status_update(
        self, entity_urn: str, status: Union[str, ShareResultStateClass]
    ) -> None:
        existing_share_aspect = self.source_graph.get_aspect(
            entity_urn, models.ShareClass
        )
        if not existing_share_aspect:
            logger.warning(f"Entity {entity_urn} doesn't have a share aspect.")
            return

        updated_share_aspect = self.update_unshare_aspect(
            unshared_urn=entity_urn,
            existing_share_aspect=existing_share_aspect,
            source_share_connection_urn=self.source_share_connection_urn,
            status=status,
        )

        # Update the share aspect for the entity
        # if it is None that means no change
        if updated_share_aspect:
            logger.info(f"Unshare Status update to {status} for {entity_urn}")
            self.source_graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=entity_urn,
                    aspect=updated_share_aspect,
                )
            )

    def unshare(
        self, entity_urn: str, lineage_direction: Optional[LineageDirection] = None
    ) -> ExecuteUnshareResult:
        if not lineage_direction:
            # If lineage direction is not provided, we need to determine the lineage direction from the share aspect.
            lineage_direction = self.get_lineage_direction_from_share_aspect(entity_urn)

        logger.info(
            f"Unsharing {entity_urn} with lineage direction {lineage_direction}"
        )
        # We have to determine the entities to unshare as it is possible a Container was shared with the entity
        entities_to_unshare = self.determine_entities_to_sync(entity_urn)

        if lineage_direction:
            if lineage_direction == LineageDirection.BOTH:
                lineage_direction_list = [
                    LineageDirection.UPSTREAM,
                    LineageDirection.DOWNSTREAM,
                ]
            else:
                lineage_direction_list = [lineage_direction]

            for lineage_direction in lineage_direction_list:
                lineage_entities = self.get_entities_across_lineage(
                    entity_urn,
                    lineage_direction=lineage_direction,
                    max_entities=MAX_ENTITIES_PER_SHARE,
                )
                entities_to_unshare = entities_to_unshare.union(lineage_entities)

        unshared_urns = set()
        count = 0

        # Adding the explicity unshare to the end of the list to ensure that the entity itself is unshared last.
        entities_to_unshare.remove(entity_urn)
        urns_to_unshare = list(entities_to_unshare) + [entity_urn]
        for urn_to_unshare in urns_to_unshare:
            unshared_urn = self.unshare_one_entity(
                unshared_urn=urn_to_unshare,
                implicit_share_entity=(
                    entity_urn if entity_urn != urn_to_unshare else None
                ),
            )
            if unshared_urn:
                unshared_urns.add(unshared_urn)
            count += 1

            if len(unshared_urns) % 10 == 0:
                logger.info(
                    f"Unshared {len(unshared_urns)} out of {len(urns_to_unshare)} entities."
                )

        unshare_result = ExecuteUnshareResult(
            status="ok",
            entities_unshared=list(unshared_urns),
        )

        self.unshare_status_update(entity_urn, ShareResultStateClass.SUCCESS)
        logger.info(f"Unshared {len(urns_to_unshare)} entities")
        return unshare_result
