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
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict
from datahub.utilities.urns._urn_base import Urn

logger = logging.getLogger(__name__)

QUERY_ENTITIES = """
query listEntities($input: ScrollAcrossEntitiesInput!) {
  scrollAcrossEntities(input: $input) {
    nextScrollId
    count
    searchResults {
      entity {
        ... on QueryEntity {
          urn
        }
        ... on DataProcessInstance {
          urn
        }
      }
    }
  }
}
"""


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
    num_soft_deleted_entity_removal_started: int = 0
    num_hard_deleted: int = 0
    num_hard_deleted_by_type: TopKDict[str, int] = field(default_factory=TopKDict)
    sample_hard_deleted_aspects_by_type: TopKDict[str, LossyList[str]] = field(
        default_factory=TopKDict
    )
    runtime_limit_reached: bool = False
    deletion_limit_reached: bool = False


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
            self.report.num_hard_deleted += 1

            current_count = self.report.num_hard_deleted_by_type.get(entity_type, 0)
            self.report.num_hard_deleted_by_type[entity_type] = current_count + 1
            if entity_type not in self.report.sample_hard_deleted_aspects_by_type:
                self.report.sample_hard_deleted_aspects_by_type[entity_type] = (
                    LossyList()
                )
            self.report.sample_hard_deleted_aspects_by_type[entity_type].append(urn)

    def delete_entity(self, urn: str) -> None:
        assert self.ctx.graph

        entity_urn = Urn.from_string(urn)
        if self.dry_run:
            logger.info(
                f"Dry run is on otherwise it would have deleted {urn} with hard deletion"
            )
            return
        if self._deletion_limit_reached() or self._times_up():
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
        # Print report every 2 minutes
        if time_taken > 120:
            self.last_print_time = time.time()
            logger.info(f"\n{self.report.as_string()}")

    def _process_futures(self, futures: Dict[Future, str]) -> Dict[Future, str]:
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        futures = {future: urn for future, urn in futures.items() if future in not_done}

        for future in done:
            self._print_report()
            if future.exception():
                self.report.failure(
                    title="Failed to delete entity",
                    message="Failed to delete entity",
                    context=futures[future],
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

    def _get_soft_deleted(self, graphql_query: str, entity_type: str) -> Iterable[str]:
        assert self.ctx.graph
        scroll_id: Optional[str] = None

        batch_size = self.config.batch_size
        if entity_type == "DATA_PROCESS_INSTANCE":
            # Due to a bug in Data process instance querying this is a temp workaround
            # to avoid a giant stacktrace by having a smaller batch size in first call
            # This will be remove in future version after server with fix has been
            # around for a while
            batch_size = 10

        while True:
            try:
                if entity_type not in self.report.num_calls_made:
                    self.report.num_calls_made[entity_type] = 1
                else:
                    self.report.num_calls_made[entity_type] += 1
                self._print_report()
                result = self.ctx.graph.execute_graphql(
                    graphql_query,
                    {
                        "input": {
                            "types": [entity_type],
                            "query": "*",
                            "scrollId": scroll_id if scroll_id else None,
                            "count": batch_size,
                            "orFilters": [
                                {
                                    "and": [
                                        {
                                            "field": "removed",
                                            "values": ["true"],
                                            "condition": "EQUAL",
                                        }
                                    ]
                                }
                            ],
                        }
                    },
                )
            except Exception as e:
                self.report.failure(
                    f"While trying to get {entity_type} with {scroll_id}", exc=e
                )
                break
            scroll_across_entities = result.get("scrollAcrossEntities")
            if not scroll_across_entities:
                break
            search_results = scroll_across_entities.get("searchResults")
            count = scroll_across_entities.get("count")
            if not count or not search_results:
                # Due to a server bug we cannot rely on just count as it was returning response like this
                # {'count': 1, 'nextScrollId': None, 'searchResults': []}
                break
            if entity_type == "DATA_PROCESS_INSTANCE":
                # Temp workaround. See note in beginning of the function
                # We make the batch size = config after call has succeeded once
                batch_size = self.config.batch_size
            scroll_id = scroll_across_entities.get("nextScrollId")
            if entity_type not in self.report.num_entities_found:
                self.report.num_entities_found[entity_type] = 0
            self.report.num_entities_found[entity_type] += scroll_across_entities.get(
                "count"
            )
            for query in search_results:
                yield query["entity"]["urn"]

    def _get_urns(self) -> Iterable[str]:
        assert self.ctx.graph
        yield from self.ctx.graph.get_urns_by_filter(
            entity_types=self.config.entity_types,
            platform=self.config.platform,
            env=self.config.env,
            query=self.config.query,
            status=RemovedStatusFilter.ONLY_SOFT_DELETED,
            batch_size=self.config.batch_size,
        )
        yield from self._get_soft_deleted(QUERY_ENTITIES, "QUERY")
        yield from self._get_soft_deleted(QUERY_ENTITIES, "DATA_PROCESS_INSTANCE")

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

        futures: Dict[Future, str] = dict()
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for urn in self._get_urns():
                self._print_report()
                while len(futures) >= self.config.futures_max_at_time:
                    futures = self._process_futures(futures)
                if self._deletion_limit_reached() or self._times_up():
                    break
                future = executor.submit(self.delete_soft_deleted_entity, urn)
                futures[future] = urn

            logger.info(f"Waiting for {len(futures)} futures to complete")
            while len(futures) > 0:
                self._print_report()
                futures = self._process_futures(futures)
