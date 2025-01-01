import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Dict, List, Optional

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
    enabled: bool = Field(
        default=True, description="Whether to do soft deletion cleanup."
    )
    retention_days: int = Field(
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

    limit_entities_delete: Optional[int] = Field(
        25000, description="Max number of entities to delete."
    )

    futures_max_at_time: int = Field(
        10000, description="Max number of futures to have at a time."
    )

    runtime_limit_seconds: int = Field(
        7200,  # 2 hours by default
        description="Runtime limit in seconds",
    )


@dataclass
class SoftDeletedEntitiesReport(SourceReport):
    num_soft_deleted_entity_processed: int = 0
    num_soft_deleted_retained_due_to_age: int = 0
    num_soft_deleted_entity_removal_started: int = 0
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
        self.start_time = 0.0
        self._report_lock: Lock = Lock()
        self.last_print_time = 0.0

    def _increment_retained_count(self) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_retained_due_to_age += 1

    def _increment_removal_started_count(self) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_entity_removal_started += 1

    def _update_report(self, urn: str, entity_type: str) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_entity_removed += 1

            current_count = self.report.num_soft_deleted_entity_removed_by_type.get(
                entity_type, 0
            )
            self.report.num_soft_deleted_entity_removed_by_type[entity_type] = (
                current_count + 1
            )
            if (
                entity_type
                not in self.report.sample_soft_deleted_removed_aspects_by_type
            ):
                self.report.sample_soft_deleted_removed_aspects_by_type[
                    entity_type
                ] = LossyList()
            self.report.sample_soft_deleted_removed_aspects_by_type[entity_type].append(
                urn
            )

    def delete_entity(self, urn: str) -> None:
        assert self.ctx.graph

        entity_urn = Urn.from_string(urn)
        if self.dry_run:
            logger.info(
                f"Dry run is on otherwise it would have deleted {urn} with hard deletion"
            )
            return
        self._increment_removal_started_count()
        self.ctx.graph.delete_entity(urn=urn, hard=True)
        self.ctx.graph.delete_references_to_urn(
            urn=urn,
            dry_run=False,
        )
        self._update_report(urn, entity_urn.entity_type)

    def delete_soft_deleted_entity(self, urn: str) -> None:
        assert self.ctx.graph

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
            else:
                self._increment_retained_count()

    def _print_report(self) -> None:
        time_taken = round(time.time() - self.last_print_time, 1)
        # Print report every 5 minutes
        if time_taken > 300:
            self.last_print_time = time.time()
            logger.info(f"\n{self.report.as_string()}")

    def _process_futures(self, futures: Dict[Future, str]) -> Dict[Future, str]:
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        futures = {future: urn for future, urn in futures.items() if future in not_done}

        for future in done:
            self._print_report()
            if future.exception():
                logger.error(
                    f"Failed to delete entity {futures[future]}: {future.exception()}"
                )
                self.report.failure(
                    f"Failed to delete entity {futures[future]}",
                    exc=future.exception(),
                )
            self.report.num_soft_deleted_entity_processed += 1
            if (
                self.report.num_soft_deleted_entity_processed % self.config.batch_size
                == 0
            ):
                if self.config.delay:
                    logger.debug(
                        f"Sleeping for {self.config.delay} seconds before further processing batch"
                    )
                    time.sleep(self.config.delay)
        return futures

    def cleanup_soft_deleted_entities(self) -> None:
        if not self.config.enabled:
            return
        assert self.ctx.graph
        self.start_time = time.time()
        urns = self.ctx.graph.get_urns_by_filter(
            entity_types=self.config.entity_types,
            platform=self.config.platform,
            env=self.config.env,
            query=self.config.query,
            status=RemovedStatusFilter.ONLY_SOFT_DELETED,
            batch_size=self.config.batch_size,
        )

        futures: Dict[Future, str] = dict()
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for urn in urns:
                self._print_report()
                while len(futures) > self.config.futures_max_at_time:
                    futures = self._process_futures(futures)
                if (
                    self.config.limit_entities_delete
                    and self.report.num_soft_deleted_entity_removed
                    > self.config.limit_entities_delete
                ):
                    logger.info(
                        f"Limit of {self.config.limit_entities_delete} entities reached. Stopped adding more."
                    )
                    break
                if (
                    self.config.runtime_limit_seconds
                    and time.time() - self.start_time
                    > self.config.runtime_limit_seconds
                ):
                    logger.info(
                        f"Runtime limit of {self.config.runtime_limit_seconds} seconds reached. Not submitting more futures."
                    )
                    break

                future = executor.submit(self.delete_soft_deleted_entity, urn)
                futures[future] = urn

            logger.info(f"Waiting for {len(futures)} futures to complete")
            while len(futures) > 0:
                self._print_report()
                futures = self._process_futures(futures)
