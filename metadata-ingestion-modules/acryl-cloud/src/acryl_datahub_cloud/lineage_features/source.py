import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Set

from opensearchpy import OpenSearch
from opensearchpy.exceptions import (
    ConnectionError as OpenSearchConnectionError,
    ConnectionTimeout,
    RequestError,
    TransportError,
)
from pydantic import validator
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from acryl_datahub_cloud.elasticsearch.config import ElasticSearchClientConfig
from acryl_datahub_cloud.elasticsearch.graph_service import ElasticGraphRow
from datahub.configuration import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.metadata.schema_classes import AuditStampClass, LineageFeaturesClass

logger = logging.getLogger(__name__)

SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system"


class LineageFeaturesSourceConfig(ConfigModel):
    enabled: bool = True
    materialize_entities: bool = False
    search_index: ElasticSearchClientConfig = ElasticSearchClientConfig()
    query_timeout: int = 30
    extract_batch_size: int = 3000
    max_retries: int = 3
    retry_delay_seconds: int = 5
    retry_backoff_multiplier: float = 2.0

    # Cleanup old features when they have not been updated for this many days
    # This is required because we only emit this feature for cases where we find a lineage
    # in the graph index
    cleanup_batch_size: int = 100
    cleanup_old_features_days: int = 2

    @validator("max_retries")
    def validate_max_retries(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_retries must be at least 1")
        return v

    @validator("retry_delay_seconds")
    def validate_retry_delay_seconds(cls, v: int) -> int:
        if v < 1:
            raise ValueError("retry_delay_seconds must be at least 1")
        return v

    @validator("retry_backoff_multiplier")
    def validate_retry_backoff_multiplier(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("retry_backoff_multiplier must be at least 1.0")
        return v


@dataclass
class LineageExtractGraphSourceReport(SourceReport, IngestionStageReport):
    valid_urns_count: int = 0
    upstream_count: int = 0
    downstream_count: int = 0
    edges_scanned: int = 0
    skipped_materialized_urns_count: int = 0
    zero_upstream_count: int = 0
    zero_downstream_count: int = 0
    has_asset_level_lineage_count: int = 0
    zero_asset_level_lineage_count: int = 0
    cleanup_old_features_time: int = 0
    cleanup_old_features_count: int = 0


@platform_name(id="datahub", platform_name="DataHub")
@config_class(LineageFeaturesSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubLineageFeaturesSource(Source):
    """
    DataHub Lineage Features Source that extracts lineage information from Elasticsearch/OpenSearch.
    """

    platform = "datahub"

    def __init__(
        self, config: LineageFeaturesSourceConfig, ctx: PipelineContext
    ) -> None:
        super().__init__(ctx)
        self.config: LineageFeaturesSourceConfig = config
        self.report = LineageExtractGraphSourceReport()
        self.opened_files: List[str] = []

        self.valid_urns: Set[str] = set()
        self.upstream_counts: Dict[str, int] = defaultdict(int)
        self.downstream_counts: Dict[str, int] = defaultdict(int)
        self.last_print_time = time.time()

    def _get_retry_decorator(
        self,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Create a retry decorator based on config parameters"""

        def should_retry_exception(exception: Exception) -> bool:
            """Custom retry predicate for OpenSearch exceptions"""
            if isinstance(
                exception,
                (
                    OpenSearchConnectionError,
                    ConnectionTimeout,
                    RequestError,
                    TransportError,
                ),
            ):
                return True
            # Also retry on general connection and timeout errors
            if isinstance(exception, (ConnectionError, TimeoutError)):
                return True
            return False

        return retry(
            retry=retry_if_exception_type(
                (
                    OpenSearchConnectionError,
                    ConnectionTimeout,
                    RequestError,
                    TransportError,
                    ConnectionError,
                    TimeoutError,
                )
            ),
            stop=stop_after_attempt(self.config.max_retries),
            wait=wait_exponential(
                multiplier=self.config.retry_backoff_multiplier,
                min=self.config.retry_delay_seconds,
                max=30,
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    def _create_pit_with_retry(self, server: OpenSearch, index: str) -> str:
        """Create a Point-in-Time (PIT) with retry logic"""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _create_pit() -> str:
            logger.debug(f"Creating PIT for index: {index}")
            response = server.create_pit(index, keep_alive="10m")
            pit = response.get("pit_id")
            if not pit:
                raise Exception("Failed to create PIT - no pit_id returned")
            logger.debug(f"Successfully created PIT: {pit}")
            return pit

        return _create_pit()

    def _search_with_retry(
        self, server: OpenSearch, query: dict, batch_size: int
    ) -> dict:
        """Execute search with retry logic"""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _search() -> dict:
            logger.debug(f"Executing search with batch size: {batch_size}")
            return server.search(
                body=query,
                size=batch_size,
                params={"timeout": self.config.query_timeout},
            )

        return _search()

    def _delete_pit_with_retry(self, server: OpenSearch, pit: str) -> None:
        """Delete Point-in-Time (PIT) with retry logic"""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _delete_pit() -> None:
            logger.debug(f"Deleting PIT: {pit}")
            server.delete_pit(body={"pit_id": pit})
            logger.debug(f"Successfully deleted PIT: {pit}")

        _delete_pit()

    def _create_opensearch_client_with_retry(self) -> OpenSearch:
        """Create OpenSearch client with retry logic"""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _create_client() -> OpenSearch:
            logger.debug(
                f"Creating OpenSearch client for endpoint: {self.config.search_index.endpoint}"
            )
            return OpenSearch(
                [self.config.search_index.endpoint],
                http_auth=(
                    self.config.search_index.username,
                    self.config.search_index.password,
                ),
                use_ssl=self.config.search_index.use_ssl,
            )

        return _create_client()

    def _update_report(self) -> None:
        """
        Information to see whether we are close to hitting the memory limits
        """
        self.report.valid_urns_count = len(self.valid_urns)
        self.report.upstream_count = len(self.upstream_counts.keys())
        self.report.downstream_count = len(self.downstream_counts.keys())

    def _print_report(self) -> None:
        """
        Printing is required like this because the report is only printed
        when the workunits are yielded
        In case of background processes we won't know the progress if this is not done
        """
        time_taken = round(time.time() - self.last_print_time, 1)
        # Print report every 2 minutes
        if time_taken > 120:
            self._update_report()
            self.last_print_time = time.time()
            logger.info(f"\n{self.report.as_string()}")

    def process_batch(self, results: Iterable[dict]) -> None:
        for doc in results:
            self._print_report()
            row = ElasticGraphRow.from_elastic_doc(doc["_source"])
            self.report.edges_scanned += 1
            if (
                row.source_urn in self.valid_urns
                and row.destination_urn in self.valid_urns
            ):
                self.upstream_counts[row.source_urn] += 1
                self.downstream_counts[row.destination_urn] += 1

    def populate_valid_urns(self) -> None:
        graph = self.ctx.require_graph("Load non soft-deleted urns")
        for urn in graph.get_urns_by_filter(batch_size=self.config.extract_batch_size):
            self._print_report()
            self.valid_urns.add(urn)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage("Load valid URNs"):
            self.populate_valid_urns()

        server = self._create_opensearch_client_with_retry()

        query: Dict[str, Any] = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"relationshipType": "Consumes"}},
                        {"term": {"relationshipType": "DownstreamOf"}},
                        {"term": {"relationshipType": "TrainedBy"}},
                        {"term": {"relationshipType": "UsedBy"}},
                        {"term": {"relationshipType": "MemberOf"}},
                        {"term": {"relationshipType": "DerivedFrom"}},
                        {"term": {"relationshipType": "Produces"}},
                        {"term": {"relationshipType": "DashboardContainsDashboard"}},
                        {
                            "bool": {
                                "must": [
                                    {"term": {"relationshipType": "Contains"}},
                                    {"term": {"source.entityType": "dashboard"}},
                                    {"term": {"destination.entityType": "chart"}},
                                ]
                            }
                        },
                    ],
                    "must_not": [
                        {"term": {"source.entityType": "schemaField"}},
                        {"term": {"destination.entityType": "schemaField"}},
                    ],
                },
            },
        }

        index = f"{self.config.search_index.index_prefix}graph_service_v1"
        pit = self._create_pit_with_retry(server, index)

        # TODO: Save PIT, we can resume processing based on <pit, search_after> tuple
        query.update({"pit": {"id": pit, "keep_alive": "10m"}})

        # TODO: Using slicing we can parallelize the ES calls below:
        # https://opensearch.org/docs/latest/search-plugins/searching-data/point-in-time/#search-slicing
        batch_size = self.config.extract_batch_size
        with self.report.new_stage("Extract lineage features"):
            try:
                while True:
                    results = self._search_with_retry(server, query, batch_size)
                    self.process_batch(results["hits"]["hits"])
                    if len(results["hits"]["hits"]) < batch_size:
                        break
                    query.update({"search_after": results["hits"]["hits"][-1]["sort"]})
            except Exception as e:
                logger.error(f"Error during lineage extraction: {e}")
                self.report.report_failure(
                    title="Lineage extraction failed",
                    message="Failed to extract lineage features from Elasticsearch",
                    context=f"Error: {str(e)}",
                    exc=e,
                )
                # Ensure PIT is cleaned up even on error
                try:
                    self._delete_pit_with_retry(server, pit)
                except Exception as cleanup_error:
                    logger.warning(
                        f"Failed to cleanup PIT after error: {cleanup_error}"
                    )
                raise
        self._update_report()
        self._delete_pit_with_retry(server, pit)

        with self.report.new_stage("emission of lineage features"):
            yield from self._emit_lineage_features()

        with self.report.new_stage("cleanup old lineage features"):
            yield from self._cleanup_old_features()

    def _cleanup_old_features(self) -> Iterable[MetadataWorkUnit]:
        """
        This is required because we only emit this feature for cases where we find a lineage
        in the graph index
        """
        cutoff_time = int(
            (time.time() - (self.config.cleanup_old_features_days * 24 * 60 * 60))
            * 1000
        )
        self.report.cleanup_old_features_time = cutoff_time

        for urn in self.ctx.require_graph("Cleanup old features").get_urns_by_filter(
            extraFilters=[
                {
                    "field": "hasAssetLevelLineageFeature",
                    "negated": False,
                    "condition": "EQUAL",
                    "values": ["true"],
                },
                {
                    "field": "lineageFeaturesComputedAt",
                    "negated": False,
                    "condition": "LESS_THAN",
                    "values": [str(cutoff_time)],
                },
            ],
            batch_size=self.config.cleanup_batch_size,
        ):
            # Emit lineage features with zero upstreams and downstreams for cleanup
            wu = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=LineageFeaturesClass(
                    upstreamCount=0,
                    downstreamCount=0,
                    hasAssetLevelLineage=False,
                    computedAt=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor=SYSTEM_ACTOR,
                    ),
                ),
            ).as_workunit()
            self.report.cleanup_old_features_count += 1
            self.report.report_workunit(wu)
            yield wu

    def _emit_lineage_features(self) -> Iterable[MetadataWorkUnit]:
        # In Python 3.9, can be replaced by `self.self.upstream_counts.keys() | self.downstream_counts.keys()`
        for urn in set(self.upstream_counts.keys()).union(
            self.downstream_counts.keys()
        ):
            if (not self.config.materialize_entities) and urn not in self.valid_urns:
                self.report.skipped_materialized_urns_count += 1
                continue
            logger.debug(
                f"{urn}: {self.upstream_counts[urn]}, {self.downstream_counts[urn]}"
            )
            if self.upstream_counts[urn] == 0:
                self.report.zero_upstream_count += 1
            if self.downstream_counts[urn] == 0:
                self.report.zero_downstream_count += 1
            has_asset_level_lineage = (
                self.upstream_counts[urn] > 0 or self.downstream_counts[urn] > 0
            )
            if has_asset_level_lineage:
                self.report.has_asset_level_lineage_count += 1
            else:
                self.report.zero_asset_level_lineage_count += 1
            wu = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=LineageFeaturesClass(
                    upstreamCount=self.upstream_counts[urn],
                    downstreamCount=self.downstream_counts[urn],
                    hasAssetLevelLineage=has_asset_level_lineage,
                    computedAt=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor=SYSTEM_ACTOR,
                    ),
                ),
            ).as_workunit()
            self.report.report_workunit(wu)
            yield wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        for file in self.opened_files:
            os.remove(file)
        return super().close()
