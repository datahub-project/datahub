import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Dict, Iterable, Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.configuration.env_vars import get_report_info_sample_size
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.urns._urn_base import Urn
from datahub.utilities.urns.error import InvalidUrnError

logger = logging.getLogger(__name__)


class QueryCleanupConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Whether to do query cleanup or not.",
    )

    retention_days: int = Field(
        30,
        description=(
            "Soft-delete SYSTEM queries whose lastModifiedAt is older than this many days. "
            "Set this to at least the largest connector ingestion window: a live query's "
            "lastModifiedAt is refreshed on every re-observation, so only queries that have "
            "aged out of every ingestion window can cross the cutoff. This is a distinct clock "
            "from soft_deleted_entities_cleanup.retention_days (which measures time since "
            "soft-deletion)."
        ),
    )

    hard_delete_entities: bool = Field(
        False,
        description=(
            "Hard-delete matched queries directly instead of soft-deleting them. The default "
            "(False) soft-deletes here and lets soft_deleted_entities_cleanup hard-delete them "
            "on a later run, which additionally calls deleteReferences to clear inbound pointers. "
            "This shortcut skips that step, so `via` pointers on lineage aspects are left "
            "pointing at the deleted query."
        ),
    )

    batch_size: int = Field(
        500,
        description="The number of entities to fetch in a batch from search.",
    )

    max_workers: int = Field(
        10,
        description="The number of workers to use for deletion.",
    )

    delay: Optional[float] = Field(
        0.25,
        description="Delay between each batch.",
    )

    futures_max_at_time: int = Field(
        1000,
        description=(
            "Max number of in-flight delete operations to have pending at a time. "
            "Bounds memory when streaming a large candidate set; the selection loop "
            "throttles once this many deletes are outstanding."
        ),
    )

    limit_entities_delete: Optional[int] = Field(
        25000,
        description="Max number of queries to delete in a single run.",
    )

    runtime_limit_seconds: int = Field(
        7200,  # 2 hours by default
        description="Runtime limit in seconds for a single run.",
    )


@dataclass
class QueryCleanupReport(SourceReport):
    num_queries_found: int = 0
    num_system_queries_found: int = 0
    num_queries_soft_deleted: int = 0
    num_queries_hard_deleted: int = 0
    sample_deleted_queries: LossyList[str] = field(
        default_factory=lambda: LossyList(max_elements=get_report_info_sample_size())
    )
    runtime_limit_reached: bool = False
    deletion_limit_reached: bool = False


class QueryCleanup:
    """
    Maintenance source that soft-deletes old SYSTEM queries by age alone (aggressive policy).

    Selection is a single server-side filter on `source == SYSTEM` and
    `lastModifiedAt < cutoff`. The existing SoftDeletedEntitiesCleanup completes the hard-delete second pass.
    """

    def __init__(
        self,
        ctx: PipelineContext,
        config: QueryCleanupConfig,
        report: QueryCleanupReport,
        dry_run: bool = False,
    ):
        if not ctx.graph:
            raise ValueError("Datahub API is required")

        self.graph: DataHubGraph = ctx.graph
        self.ctx = ctx
        self.config = config
        self.report = report
        self.dry_run = dry_run
        self.start_time = time.time()
        self._report_lock: Lock = Lock()
        self.last_print_time = 0.0

    def _get_urns(self) -> Iterable[str]:
        cutoff_millis = int(
            (
                datetime.now(timezone.utc).timestamp()
                - self.config.retention_days * 24 * 60 * 60
            )
            * 1000
        )
        yield from self.graph.get_urns_by_filter(
            entity_types=["query"],
            status=RemovedStatusFilter.NOT_SOFT_DELETED,
            batch_size=self.config.batch_size,
            extraFilters=[
                SearchFilterRule(
                    field="source",
                    condition="EQUAL",
                    values=["SYSTEM"],
                ).to_raw(),
                SearchFilterRule(
                    field="lastModifiedAt",
                    condition="LESS_THAN",
                    values=[f"{cutoff_millis}"],
                ).to_raw(),
            ],
        )

    def _update_report(self, urn: str) -> None:
        """Thread-safe method to update report fields after a delete."""
        with self._report_lock:
            if self.config.hard_delete_entities:
                self.report.num_queries_hard_deleted += 1
            else:
                self.report.num_queries_soft_deleted += 1
            self.report.sample_deleted_queries.append(urn)

    def delete_query(self, urn: Urn) -> None:
        if self.dry_run:
            logger.info(
                f"Dry run is on, otherwise it would have deleted {urn} "
                f"(hard_delete={self.config.hard_delete_entities})"
            )
            return
        if self._deletion_limit_reached() or self._times_up():
            return
        self.graph.delete_entity(urn=urn.urn(), hard=self.config.hard_delete_entities)
        self._update_report(urn.urn())

    def _print_report(self) -> None:
        time_taken = round(time.time() - self.last_print_time, 1)
        # Print report every 2 minutes
        if time_taken > 120:
            self.last_print_time = time.time()
            logger.info(f"\n{self.report.as_string()}")

    def _process_futures(self, futures: Dict[Future, Urn]) -> Dict[Future, Urn]:
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)

        for future in done:
            self._print_report()
            if future.exception():
                self.report.failure(
                    title="Failed to delete query",
                    message="Failed to delete query",
                    context=futures[future].urn(),
                    exc=future.exception(),
                )

        remaining = {
            future: urn for future, urn in futures.items() if future in not_done
        }
        if done and self.config.delay:
            logger.debug(
                f"Sleeping for {self.config.delay} seconds before processing next batch"
            )
            time.sleep(self.config.delay)
        return remaining

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
        num_deleted = (
            self.report.num_queries_soft_deleted + self.report.num_queries_hard_deleted
        )
        if (
            self.config.limit_entities_delete
            and num_deleted > self.config.limit_entities_delete
        ):
            with self._report_lock:
                self.report.deletion_limit_reached = True
            return True
        return False

    def cleanup_queries(self) -> None:
        if not self.config.enabled:
            return
        self.start_time = time.time()

        futures: Dict[Future, Urn] = dict()
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for urn in self._get_urns():
                try:
                    self.report.num_queries_found += 1
                    self.report.num_system_queries_found += 1
                    query_urn = Urn.from_string(urn)
                except InvalidUrnError as e:
                    logger.error(f"Failed to parse urn {urn} with error {e}")
                    continue

                self._print_report()
                while len(futures) >= self.config.futures_max_at_time:
                    futures = self._process_futures(futures)
                if self._deletion_limit_reached() or self._times_up():
                    break
                future = executor.submit(self.delete_query, query_urn)
                futures[future] = query_urn

            logger.info(f"Waiting for {len(futures)} futures to complete")
            while len(futures) > 0:
                self._print_report()
                futures = self._process_futures(futures)
