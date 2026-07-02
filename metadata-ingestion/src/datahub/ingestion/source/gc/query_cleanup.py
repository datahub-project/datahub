import logging
import time
from concurrent.futures import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.configuration.env_vars import get_report_info_sample_size
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.metadata.schema_classes import StatusClass
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
            "pointing at the deleted query. Hard deletes are parallelized across "
            "max_workers workers; soft deletes are emitted as workunits and batched by the sink."
        ),
    )

    batch_size: int = Field(
        500,
        description="The number of entities to fetch in a batch from search.",
    )

    max_workers: int = Field(
        10,
        description=(
            "Number of worker threads for parallel hard deletion. Only applies when "
            "hard_delete_entities is set; soft deletes go through the sink."
        ),
    )

    limit_entities_delete: Optional[int] = Field(
        25000,
        description=(
            "Approximate max number of queries to delete in a single run. For hard delete, "
            "the cap is checked before each submission, so concurrent workers may overshoot "
            "it by up to max_workers entities."
        ),
    )

    runtime_limit_seconds: int = Field(
        7200,  # 2 hours by default
        description="Runtime limit in seconds for a single run.",
    )


@dataclass
class QueryCleanupReport(SourceReport):
    num_queries_found: int = 0
    num_queries_soft_deleted: int = 0
    num_queries_hard_deleted: int = 0
    sample_deleted_queries: LossyList[str] = field(
        default_factory=lambda: LossyList(max_elements=get_report_info_sample_size())
    )
    qc_runtime_limit_reached: bool = False
    qc_deletion_limit_reached: bool = False

    def report_query_soft_deleted(self, urn: str) -> None:
        self.num_queries_soft_deleted += 1
        self.sample_deleted_queries.append(urn)

    def report_query_hard_deleted(self, urn: str) -> None:
        self.num_queries_hard_deleted += 1
        self.sample_deleted_queries.append(urn)


class QueryCleanup:
    """
    Maintenance source that soft-deletes old SYSTEM queries by age alone (aggressive policy).

    Selection is a single server-side filter on `source == SYSTEM` and
    `lastModifiedAt < cutoff`. Soft deletes are emitted as status workunits and left to the
    sink to batch and write (the same mechanism as stale-entity removal); the existing
    SoftDeletedEntitiesCleanup completes the hard-delete second pass. Hard deletes, when
    enabled, have no bulk or async endpoint, so they are parallelized across a worker pool.
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
        self.config = config
        self.report = report
        self.dry_run = dry_run
        self.start_time = time.time()

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

    def _times_up(self) -> bool:
        if (
            self.config.runtime_limit_seconds
            and time.time() - self.start_time > self.config.runtime_limit_seconds
        ):
            self.report.qc_runtime_limit_reached = True
            return True
        return False

    def _deletion_limit_reached(self) -> bool:
        num_deleted = (
            self.report.num_queries_soft_deleted + self.report.num_queries_hard_deleted
        )
        if (
            self.config.limit_entities_delete
            and num_deleted >= self.config.limit_entities_delete
        ):
            self.report.qc_deletion_limit_reached = True
            return True
        return False

    def _iter_candidate_urns(self) -> Iterable[Urn]:
        for urn in self._get_urns():
            try:
                query_urn = Urn.from_string(urn)
            except InvalidUrnError as e:
                self.report.warning(
                    title="Skipped query with unparseable urn",
                    message="Search returned a query urn that could not be parsed; skipping it.",
                    context=urn,
                    exc=e,
                )
                continue
            self.report.num_queries_found += 1
            yield query_urn

    def _preview_candidates(self) -> None:
        for query_urn in self._iter_candidate_urns():
            if self._deletion_limit_reached() or self._times_up():
                break
            logger.info(
                f"Dry run is on, otherwise it would have deleted {query_urn} "
                f"(hard_delete={self.config.hard_delete_entities})"
            )
            # Record the candidate so a dry run still previews what would go.
            self.report.sample_deleted_queries.append(query_urn.urn())

    def _hard_delete_one(self, urn: Urn) -> None:
        self.graph.delete_entity(urn=urn.urn(), hard=True)

    def _collect_hard_deletes(
        self, futures: Dict["Future[None]", Urn], return_when: str
    ) -> Dict["Future[None]", Urn]:
        done, not_done = wait(futures, return_when=return_when)
        for future in done:
            urn = futures[future]
            exc = future.exception()
            if exc is not None:
                self.report.warning(
                    title="Failed to hard delete query",
                    message="Failed to hard delete query",
                    context=urn.urn(),
                    exc=exc,
                )
            else:
                self.report.report_query_hard_deleted(urn.urn())
        return {f: futures[f] for f in not_done}

    def _hard_delete_queries(self) -> None:
        # Hard delete has no bulk or async path, so parallelize the per-URN blocking
        # calls across a worker pool. Completed futures are drained here on the calling
        # thread, so report updates need no locking.
        futures: Dict["Future[None]", Urn] = {}
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for query_urn in self._iter_candidate_urns():
                if self._deletion_limit_reached() or self._times_up():
                    break
                futures[executor.submit(self._hard_delete_one, query_urn)] = query_urn
                # Bound in-flight work and let completed deletes advance the report so
                # the deletion/runtime caps can trip mid-stream.
                if len(futures) >= self.config.max_workers:
                    futures = self._collect_hard_deletes(futures, FIRST_COMPLETED)
            self._collect_hard_deletes(futures, ALL_COMPLETED)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.enabled:
            return
        self.start_time = time.time()

        if self.dry_run:
            self._preview_candidates()
            return
        if self.config.hard_delete_entities:
            self._hard_delete_queries()
            return

        # Soft delete is a plain status write, so hand each to the sink as a workunit
        # and let it batch/async the write (like stale-entity removal).
        for query_urn in self._iter_candidate_urns():
            if self._deletion_limit_reached() or self._times_up():
                break
            self.report.report_query_soft_deleted(query_urn.urn())
            yield MetadataChangeProposalWrapper(
                entityUrn=query_urn.urn(),
                aspect=StatusClass(removed=True),
            ).as_workunit()
