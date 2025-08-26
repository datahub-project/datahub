import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from acryl_datahub_cloud.datahub_metadata_sharing.query import (
    GRAPHQL_SCROLL_SHARED_ENTITIES,
    GRAPHQL_SHARE_ENTITY,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


class GraphQLError(Exception):
    """Custom exception for GraphQL-specific errors"""

    pass


class DataHubMetadataSharingSourceConfig(BaseModel):
    batch_size: int = 100
    batch_delay_ms: int = 100
    max_retries: int = 3
    initial_retry_delay_ms: int = 1000


@dataclass
class DataHubMetadataSharingSourceReport(SourceReport):
    entities_shared: int = 0
    entities_failed: int = 0
    implicit_entities_skipped: int = 0
    batches_processed: int = 0


@platform_name(id="datahub", platform_name="DataHub")
@config_class(DataHubMetadataSharingSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubMetadataSharingSource(Source):
    """MetadataSharing Source that reshares entities across DataHub instances"""

    def __init__(
        self, config: DataHubMetadataSharingSourceConfig, ctx: PipelineContext
    ):
        super().__init__(ctx)
        self.config: DataHubMetadataSharingSourceConfig = config
        self.report = DataHubMetadataSharingSourceReport()
        self.graph: Optional[DataHubGraph] = None

    @retry(
        retry=retry_if_exception_type((GraphQLError, ConnectionError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def execute_graphql_with_retry(
        self, query: str, variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute GraphQL query with retry logic"""
        if self.graph is None:
            raise ValueError("Graph client not initialized")
        response = self.graph.execute_graphql(query, variables=variables)
        error = response.get("error")
        if error:
            raise GraphQLError(f"GraphQL error: {error}")
        return response

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.graph = self.ctx.require_graph("Loading default graph coordinates.")

        self.reshare_entities()

        # This source doesn't produce any work units
        return []

    def reshare_entities(self) -> None:
        scroll_id: Optional[str] = None
        current_batch_number: int = 1

        try:
            while True:
                next_scroll_id, results = self.scroll_shared_entities(
                    scroll_id, self.config.batch_size
                )

                for result in results:
                    self._process_single_entity(result)

                self.report.batches_processed = current_batch_number
                logger.info(
                    f"Completed sharing batch of entities {current_batch_number} of size {self.config.batch_size}!"
                )
                current_batch_number += 1

                if next_scroll_id is None:
                    break
                else:
                    scroll_id = next_scroll_id

                time.sleep(self.config.batch_delay_ms / 1000.0)

        except Exception as e:
            self.report.report_failure(
                title="Failed to process batches",
                message="Error occurred while processing one or more batches!",
                context=f"message = {str(e)}",
                exc=e,
            )
            return

        logger.info(
            f"Successfully shared {self.report.entities_shared} entities, failed to share {self.report.entities_failed} entities.",
        )

    # Rest of the methods remain the same...

    def _process_single_entity(self, result: Dict[str, Any]) -> None:
        """Process a single entity result"""
        entity_urn = result.get("entity", {}).get("urn", None)
        share_results = (
            result.get("entity", {}).get("share", {}).get("lastShareResults", [])
        )

        if entity_urn is None:
            self.report.report_warning(
                message="Failed to resolve entity urn for shared asset! Skipping...",
                context=f"Response: {str(result)}",
            )
            return

        for share_result in share_results:
            try:
                destination_data = share_result.get("destination", {})
                destination_urn = destination_data.get("urn", "")
                previous_status = share_result.get("status")
                share_config = share_result.get("shareConfig", {})

                # Important: If there is implicit entity, we should skip this urn.
                # This means the entity was not EXPLICITLY shared, so we do not want to explicitly share here.
                implicit_shared_entity = share_result.get("implicitShareEntity")
                is_implicitly_shared = (
                    implicit_shared_entity is not None
                    and "urn" in implicit_shared_entity
                )

                if is_implicitly_shared:
                    self.report.implicit_entities_skipped += 1
                    continue

                if previous_status != "SUCCESS":
                    logger.info(
                        "Attempting to share a previously unsuccessful shared entity! "
                        + f"entity urn: {entity_urn}, destination urn: {destination_urn}"
                    )

                lineage_direction = self._determine_lineage_direction(share_config)

                shared = self.share_entity(
                    entity_urn=entity_urn,
                    destination_urn=destination_urn,
                    lineage_direction=lineage_direction,
                )

                if shared:
                    self.report.entities_shared += 1
                else:
                    self.report.entities_failed += 1

            except Exception as e:
                self.report.report_warning(
                    message="Failed to share single entity!",
                    context=f"entity urn: {entity_urn}",
                )
                logger.exception(f"Error processing entity {entity_urn}", e)
                self.report.entities_failed += 1

    def _determine_lineage_direction(
        self, share_config: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        if share_config is None:
            return None

        """Determine lineage direction based on share config"""
        include_upstreams = share_config.get("enableUpstreamLineage", False)
        include_downstreams = share_config.get(
            "enableDownstreamLineage", False
        )  # Fixed typo

        if include_upstreams and include_downstreams:
            return "BOTH"
        if include_upstreams:
            return "UPSTREAM"
        if include_downstreams:
            return "DOWNSTREAM"
        return None

    def scroll_shared_entities(
        self, scroll_id: Optional[str], count: int
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """Scroll through shared entities with retry logic"""
        response = self.execute_graphql_with_retry(
            GRAPHQL_SCROLL_SHARED_ENTITIES,
            variables={
                "scrollId": scroll_id,
                "count": count,
            },
        )

        result = response.get("scrollAcrossEntities", {})
        return result.get("nextScrollId"), result.get("searchResults", [])

    def share_entity(
        self, entity_urn: str, destination_urn: str, lineage_direction: Optional[str]
    ) -> bool:
        """Share entity with retry logic"""
        try:
            response = self.execute_graphql_with_retry(
                GRAPHQL_SHARE_ENTITY,
                variables={
                    "entityUrn": entity_urn,
                    "destinationUrn": destination_urn,
                    "lineageDirection": lineage_direction,
                },
            )

            result = response.get("shareEntity", {})
            if not result.get("succeeded", False):
                self.report.report_failure(
                    title="Failed to Share Entity",
                    message="Response returned that success failed for entity and destination!",
                    context=f"entity urn: {entity_urn}, destination urn: {destination_urn}",
                )
                return False

            return True

        except Exception as e:
            self.report.report_failure(
                title="Failed to Share Entity",
                message="Exception occurred while sharing entity",
                context=f"entity urn: {entity_urn}, destination urn: {destination_urn}",
                exc=e,
            )
            return False

    def get_report(self) -> SourceReport:
        return self.report
