import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Dict, Iterable, List, Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict
from datahub.utilities.urns._urn_base import Urn
from datahub.utilities.urns.error import InvalidUrnError

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
        # A default value is required otherwise QUERY and DATAPROCESS_INSTANCE won't be included
        default=[
            "dataset",
            "dashboard",
            "chart",
            "mlmodel",
            "mlmodelGroup",
            "mlfeatureTable",
            "mlfeature",
            "mlprimaryKey",
            "dataFlow",
            "dataJob",
            "glossaryTerm",
            "glossaryNode",
            "tag",
            "role",
            "corpuser",
            "corpGroup",
            "container",
            "domain",
            "dataProduct",
            "notebook",
            "businessAttribute",
            "schemaField",
            "query",
            "dataProcessInstance",
        ],
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
        1000, description="Max number of futures to have at a time."
    )

    runtime_limit_seconds: int = Field(
        7200,  # 2 hours by default
        description="Runtime limit in seconds",
    )


@dataclass
class SoftDeletedEntitiesReport(SourceReport):
    num_calls_made: Dict[str, int] = field(default_factory=dict)
    num_entities_found: Dict[str, int] = field(default_factory=dict)
    num_soft_deleted_entity_processed: int = 0
    num_soft_deleted_retained_due_to_age: int = 0
    num_soft_deleted_retained_due_to_age_by_type: TopKDict[str, int] = field(
        default_factory=TopKDict
    )
    num_soft_deleted_entity_removal_started: int = 0
    num_hard_deleted: int = 0
    num_hard_deleted_by_type: TopKDict[str, int] = field(default_factory=TopKDict)
    sample_hard_deleted_aspects_by_type: TopKDict[str, LossyList[str]] = field(
        default_factory=TopKDict
    )
    runtime_limit_reached: bool = False
    deletion_limit_reached: bool = False
    num_soft_deleted_entity_found: int = 0
    num_soft_deleted_entity_invalid_urn: int = 0


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
        self.start_time = time.time()
        self._report_lock: Lock = Lock()
        self.last_print_time = 0.0

    def _increment_retained_count(self) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_retained_due_to_age += 1

    def _increment_retained_by_type(self, type: str) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_retained_due_to_age_by_type[type] = (
                self.report.num_soft_deleted_retained_due_to_age_by_type.get(type, 0)
                + 1
            )

    def _increment_removal_started_count(self) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_entity_removal_started += 1

    def _update_report(self, urn: str, entity_type: str) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_hard_deleted += 1

            current_count = self.report.num_hard_deleted_by_type.get(entity_type, 0)
            self.report.num_hard_deleted_by_type[entity_type] = current_count + 1
            if entity_type not in self.report.sample_hard_deleted_aspects_by_type:
                self.report.sample_hard_deleted_aspects_by_type[entity_type] = (
                    LossyList()
                )
            self.report.sample_hard_deleted_aspects_by_type[entity_type].append(urn)

    def delete_entity(self, urn: Urn) -> None:
        assert self.ctx.graph

        if self.dry_run:
            logger.info(
                f"Dry run is on otherwise it would have deleted {urn} with hard deletion"
            )
            return
        if self._deletion_limit_reached() or self._times_up():
            return
        self._increment_removal_started_count()
        self.ctx.graph.delete_entity(urn=urn.urn(), hard=True)
        self.ctx.graph.delete_references_to_urn(
            urn=urn.urn(),
            dry_run=False,
        )
        self._update_report(urn.urn(), urn.entity_type)

    def delete_soft_deleted_entity(self, urn: Urn) -> None:
        assert self.ctx.graph

        retention_time = (
            int(datetime.now(timezone.utc).timestamp())
            - self.config.retention_days * 24 * 60 * 60
        )

        aspect = self.ctx.graph.get_entity_raw(entity_urn=urn.urn(), aspects=["status"])
        if "status" in aspect["aspects"]:
            if aspect["aspects"]["status"]["value"]["removed"] and aspect["aspects"][
                "status"
            ]["created"]["time"] < (retention_time * 1000):
                logger.debug(f"Hard deleting {urn}")
                self.delete_entity(urn)
            else:
                self._increment_retained_count()
                self._increment_retained_by_type(urn.entity_type)

    def _print_report(self) -> None:
        time_taken = round(time.time() - self.last_print_time, 1)
        # Print report every 2 minutes
        if time_taken > 120:
            self.last_print_time = time.time()
            logger.info(f"\n{self.report.as_string()}")

    def _process_futures(self, futures: Dict[Future, Urn]) -> Dict[Future, Urn]:
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        futures = {future: urn for future, urn in futures.items() if future in not_done}

        for future in done:
            self._print_report()
            if future.exception():
                self.report.failure(
                    title="Failed to delete entity",
                    message="Failed to delete entity",
                    context=futures[future].urn(),
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

    def _get_urns(self) -> Iterable[str]:
        assert self.ctx.graph
        # Entities created in the retention period are not considered for deletion
        created_from = int(
            (
                datetime.now(timezone.utc).timestamp()
                - self.config.retention_days * 24 * 60 * 60
            )
            * 1000
        )

        entity_types = self.config.entity_types
        # dataProcessInstance is a special case where we need to get the entities separately
        # because we need to filter based on created time we don't stream to many dataProcessInstance entities at once
        # Gc source soft-deletes dataProcessInstance entities which causes to have a lot of soft deleted entities
        if (
            self.config.entity_types
            and "dataProcessInstance" in self.config.entity_types
        ):
            entity_types = self.config.entity_types.copy()
            yield from self.ctx.graph.get_urns_by_filter(
                entity_types=["dataProcessInstance"],
                platform=self.config.platform,
                env=self.config.env,
                query=self.config.query,
                status=RemovedStatusFilter.ONLY_SOFT_DELETED,
                batch_size=self.config.batch_size,
                extraFilters=[
                    SearchFilterRule(
                        field="created",
                        condition="LESS_THAN",
                        values=[f"{created_from}"],
                    ).to_raw()
                ],
            )

            entity_types.remove("dataProcessInstance")

        yield from self.ctx.graph.get_urns_by_filter(
            entity_types=entity_types,
            platform=self.config.platform,
            env=self.config.env,
            query=self.config.query,
            status=RemovedStatusFilter.ONLY_SOFT_DELETED,
            batch_size=self.config.batch_size,
        )

    def _times_up(self) -> bool:
        if (
            self.config.runtime_limit_seconds
            and time.time() - self.start_time > self.config.runtime_limit_seconds
        ):
            with self._report_lock:
                self.report.runtime_limit_reached = True
            return True
        return False

    def _deletion_limit_reached(self) -> bool:
        if (
            self.config.limit_entities_delete
            and self.report.num_hard_deleted > self.config.limit_entities_delete
        ):
            with self._report_lock:
                self.report.deletion_limit_reached = True
            return True
        return False

    def cleanup_soft_deleted_entities(self) -> None:
        if not self.config.enabled:
            return
        self.start_time = time.time()

        futures: Dict[Future, Urn] = dict()
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for urn in self._get_urns():
                try:
                    self.report.num_soft_deleted_entity_found += 1
                    soft_deleted_urn = Urn.from_string(urn)
                except InvalidUrnError as e:
                    logger.error(f"Failed to parse urn {urn} with error {e}")
                    self.report.num_soft_deleted_entity_invalid_urn += 1
                    continue

                self._print_report()
                while len(futures) >= self.config.futures_max_at_time:
                    futures = self._process_futures(futures)
                if self._deletion_limit_reached() or self._times_up():
                    break
                future = executor.submit(
                    self.delete_soft_deleted_entity, soft_deleted_urn
                )
                futures[future] = soft_deleted_urn

            logger.info(f"Waiting for {len(futures)} futures to complete")
            while len(futures) > 0:
                self._print_report()
                futures = self._process_futures(futures)
