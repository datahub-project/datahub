import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Optional

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
        ge=1,
        description=(
            "Soft-delete SYSTEM queries whose lastModifiedAt is older than this many days. "
            "Set this to at least the largest connector ingestion window: a live query's "
            "lastModifiedAt is refreshed on every re-observation, so only queries that have "
            "aged out of every ingestion window can cross the cutoff. This is a distinct clock "
            "from soft_deleted_entities_cleanup.retention_days (which measures time since "
            "soft-deletion)."
        ),
    )

    batch_size: int = Field(
        500,
        ge=1,
        description="The number of entities to fetch in a batch from search.",
    )

    limit_entities_delete: Optional[int] = Field(
        25000,
        ge=1,
        description="Approximate max number of queries to soft-delete in a single run.",
    )

    runtime_limit_seconds: int = Field(
        7200,  # 2 hours by default
        ge=1,
        description="Runtime limit in seconds for a single run.",
    )


@dataclass
class QueryCleanupReport(SourceReport):
    num_queries_found: int = 0
    num_queries_soft_deleted: int = 0
    sample_deleted_queries: LossyList[str] = field(
        default_factory=lambda: LossyList(max_elements=get_report_info_sample_size())
    )
    qc_runtime_limit_reached: bool = False
    qc_deletion_limit_reached: bool = False

    def report_query_soft_deleted(self, urn: str) -> None:
        self.num_queries_soft_deleted += 1
        self.sample_deleted_queries.append(urn)


class QueryCleanup:
    """
    Maintenance source that soft-deletes old SYSTEM queries by age alone (aggressive policy).

    Selection is a single server-side filter on `source == SYSTEM` and
    `lastModifiedAt < cutoff`. Soft deletes are emitted as status workunits and left to the
    sink to batch and write (the same mechanism as stale-entity removal); the existing
    SoftDeletedEntitiesCleanup completes the hard-delete second pass.
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
            # Oldest-first so a capped run (limit_entities_delete / runtime_limit_seconds)
            # deletes the most-stale queries rather than an arbitrary scroll slice, and the
            # dry-run preview reflects what a real run would delete.
            sort_by="lastModifiedAt",
            sort_order="ASCENDING",
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
        if (
            self.config.limit_entities_delete
            and self.report.num_queries_soft_deleted
            >= self.config.limit_entities_delete
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
            logger.info(f"Dry run is on, otherwise it would have deleted {query_urn}")
            # Record the candidate so a dry run still previews what would go.
            self.report.sample_deleted_queries.append(query_urn.urn())

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.enabled:
            return
        self.start_time = time.time()

        if self.dry_run:
            self._preview_candidates()
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
