import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict
from datahub.utilities.urns._urn_base import Urn

logger = logging.getLogger(__name__)


class SoftDeletedEntitiesCleanupConfig(ConfigModel):
    retention_days: Optional[int] = Field(
        10,
        description="Number of days to retain metadata in DataHub",
    )

    batch_size: int = Field(
        500,
        description="The number of entities to get in a batch from GraphQL",
    )

    delay: Optional[float] = Field(
        0.25,
        description="Delay between each batch",
    )

    max_workers: int = Field(
        10,
        description="The number of workers to use for deletion",
    )

    entity_types: Optional[List[str]] = Field(
        default=None,
        description="List of entity types to cleanup",
    )

    platform: Optional[str] = Field(
        default=None,
        description="Platform to cleanup",
    )

    env: Optional[str] = Field(
        default=None,
        description="Environment to cleanup",
    )

    query: Optional[str] = Field(
        default=None,
        description="Query to filter entities",
    )


@dataclass
class SoftDeletedEntitiesReport(SourceReport):
    num_soft_deleted_entity_removed: int = 0
    num_soft_deleted_entity_removed_by_type: TopKDict[str, int] = field(
        default_factory=TopKDict
    )
    sample_soft_deleted_removed_aspects_by_type: TopKDict[str, LossyList[str]] = field(
        default_factory=TopKDict
    )


class SoftDeletedEntitiesCleanup:
    """
    Maintenance source to cleanup soft deleted entities in DataHub
    """

    def __init__(
        self,
        ctx: PipelineContext,
        config: SoftDeletedEntitiesCleanupConfig,
        report: SoftDeletedEntitiesReport,
        dry_run: bool = False,
    ):
        if not ctx.graph:
            raise ValueError(" Datahub API is required")

        self.graph: DataHubGraph = ctx.graph
        self.ctx = ctx
        self.config = config
        self.report = report
        self.dry_run = dry_run

    def delete_entity(self, urn: str) -> None:
        assert self.ctx.graph

        entity_urn = Urn.create_from_string(urn)
        self.report.num_soft_deleted_entity_removed += 1
        self.report.num_soft_deleted_entity_removed_by_type[entity_urn.entity_type] = (
            self.report.num_soft_deleted_entity_removed_by_type.get(
                entity_urn.entity_type, 0
            )
            + 1
        )
        if (
            entity_urn.entity_type
            not in self.report.sample_soft_deleted_removed_aspects_by_type
        ):
            self.report.sample_soft_deleted_removed_aspects_by_type[
                entity_urn.entity_type
            ] = LossyList()
        self.report.sample_soft_deleted_removed_aspects_by_type[
            entity_urn.entity_type
        ].append(urn)

        if self.dry_run:
            logger.info(
                f"Dry run is on otherwise it would have deleted {urn} with hard deletion"
            )
            return

        self.ctx.graph.delete_entity(urn=urn, hard=True)

    def delete_soft_deleted_entity(self, urn: str) -> None:
        assert self.ctx.graph

        if self.config.retention_days is None:
            logger.info("Retention days is not set, skipping soft delete cleanup")
            return

        retention_time = (
            int(datetime.now(timezone.utc).timestamp())
            - self.config.retention_days * 24 * 60 * 60
        )

        aspect = self.ctx.graph.get_entity_raw(entity_urn=urn, aspects=["status"])
        if "status" in aspect["aspects"]:
            if aspect["aspects"]["status"]["value"]["removed"] and aspect["aspects"][
                "status"
            ]["created"]["time"] < (retention_time * 1000):
                logger.debug(f"Hard deleting {urn}")
                self.delete_entity(urn)

    def cleanup_soft_deleted_entities(self) -> None:
        assert self.ctx.graph

        deleted_count_retention = 0
        urns = self.ctx.graph.get_urns_by_filter(
            entity_types=self.config.entity_types,
            platform=self.config.platform,
            env=self.config.env,
            query=self.config.query,
            status=RemovedStatusFilter.ONLY_SOFT_DELETED,
            batch_size=self.config.batch_size,
        )

        futures = {}
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for urn in urns:
                future = executor.submit(self.delete_soft_deleted_entity, urn)
                futures[future] = urn

            if not futures:
                return
            for future in as_completed(futures):
                if future.exception():
                    logger.error(
                        f"Failed to delete entity {futures[future]}: {future.exception()}"
                    )
                    self.report.failure(
                        f"Failed to delete entity {futures[future]}",
                        exc=future.exception(),
                    )
                deleted_count_retention += 1

                if deleted_count_retention % self.config.batch_size == 0:
                    logger.info(
                        f"Processed {deleted_count_retention} soft deleted entity and deleted {self.report.num_soft_deleted_entity_removed} entities so far"
                    )

                    if self.config.delay:
                        logger.debug(
                            f"Sleeping for {self.config.delay} seconds before getting next batch"
                        )
                        time.sleep(self.config.delay)
