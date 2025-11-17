import logging
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

from opensearchpy import OpenSearch
from opensearchpy.exceptions import (
    ConnectionError as OpenSearchConnectionError,
    ConnectionTimeout,
    RequestError,
    TransportError,
)
from pydantic import field_validator
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

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_retries must be at least 1")
        return v

    @field_validator("retry_delay_seconds")
    @classmethod
    def validate_retry_delay_seconds(cls, v: int) -> int:
        if v < 1:
            raise ValueError("retry_delay_seconds must be at least 1")
        return v

    @field_validator("retry_backoff_multiplier")
    @classmethod
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
        # Lock for thread-safe updates to shared state
        self._process_lock = threading.Lock()

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

    def _search_with_retry(
        self,
        server: OpenSearch,
        index: str,
        query: dict,
        batch_size: int,
        scroll: Optional[str] = None,
    ) -> dict:
        """Execute search with retry logic"""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _search() -> dict:
            logger.debug(f"Executing search with batch size: {batch_size}")
            search_params: dict = {"timeout": self.config.query_timeout}
            if scroll:
                search_params["scroll"] = scroll
            return server.search(
                index=index,
                body=query,
                size=batch_size,
                params=search_params,
            )

        return _search()

    def _scroll_with_retry(
        self, server: OpenSearch, scroll_id: str, scroll: str = "10m"
    ) -> dict:
        """Execute scroll with retry logic"""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _scroll() -> dict:
            logger.debug(f"Executing scroll with scroll_id: {scroll_id}")
            return server.scroll(
                scroll_id=scroll_id,
                scroll=scroll,
                params={"timeout": self.config.query_timeout},
            )

        return _scroll()

    def _clear_scroll_with_retry(self, server: OpenSearch, scroll_id: str) -> None:
        """Clear scroll context with retry logic"""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _clear_scroll() -> None:
            logger.debug(f"Clearing scroll: {scroll_id}")
            server.clear_scroll(scroll_id=scroll_id)
            logger.debug(f"Successfully cleared scroll: {scroll_id}")

        _clear_scroll()

    def _create_opensearch_client_with_retry(self) -> OpenSearch:
        """Create OpenSearch client with retry logic"""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _create_client() -> OpenSearch:
            logger.info(
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

    def _get_index_shard_count_with_retry(self, server: OpenSearch, index: str) -> int:
        """Get the number of primary shards for an index with retry logic.

        Handles both direct index names and aliases. If an alias is provided,
        the actual index name is resolved from the response.
        """
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _get_shard_count() -> int:
            logger.debug(f"Getting shard count for index/alias: {index}")
            index_settings = server.indices.get_settings(index=index)

            # Handle alias resolution: get_settings returns the actual index name as key
            # even if we pass an alias. Get the first (and typically only) key from the response.
            actual_index_names = list(index_settings.keys())
            if not actual_index_names:
                raise ValueError(f"No index found for: {index}")

            # If alias resolves to multiple indices, use the first one
            # (shouldn't happen with proper alias configuration, but handle gracefully)
            actual_index_name = actual_index_names[0]
            if len(actual_index_names) > 1:
                logger.warning(
                    f"Alias {index} resolves to {len(actual_index_names)} indices: {actual_index_names}. "
                    f"Using first index: {actual_index_name}"
                )

            # Extract number_of_shards from the settings
            # The structure is: index_settings[index_name]['settings']['index']['number_of_shards']
            number_of_shards = int(
                index_settings[actual_index_name]["settings"]["index"][
                    "number_of_shards"
                ]
            )

            if actual_index_name != index:
                logger.info(
                    f"Alias {index} resolved to index {actual_index_name}, which has {number_of_shards} primary shards"
                )
            else:
                logger.info(f"Index {index} has {number_of_shards} primary shards")

            return number_of_shards

        return _get_shard_count()

    def _get_data_node_count_with_retry(self, server: OpenSearch) -> int:
        """Get the number of data nodes in the cluster with retry logic."""
        retry_decorator = self._get_retry_decorator()

        @retry_decorator
        def _get_data_node_count() -> int:
            logger.debug("Getting data node count from cluster")
            # Get node information including roles
            nodes_info = server.nodes.info()

            data_node_count = 0
            nodes = nodes_info.get("nodes", {})

            if not nodes:
                logger.warning("No nodes found in cluster info")
                return 0

            for _, node_info in nodes.items():
                # Check if node has data role
                # Roles can be a list of strings or a dict with boolean flags
                roles = node_info.get("roles", [])

                # Handle both list format ["data", "ingest"] and dict format {"data": true, "ingest": true}
                is_data_node = False
                if isinstance(roles, list):
                    # List format: check if any data role is present
                    is_data_node = any(
                        role in roles
                        for role in [
                            "data",
                            "data_content",
                            "data_hot",
                            "data_warm",
                            "data_cold",
                        ]
                    )
                elif isinstance(roles, dict):
                    # Dict format: check if any data role is True
                    is_data_node = any(
                        roles.get(role, False)
                        for role in [
                            "data",
                            "data_content",
                            "data_hot",
                            "data_warm",
                            "data_cold",
                        ]
                    )

                if is_data_node:
                    data_node_count += 1

            if data_node_count == 0:
                logger.warning(
                    "No data nodes detected in cluster - will use single-threaded processing"
                )
            else:
                logger.info(f"Cluster has {data_node_count} data node(s)")

            return data_node_count

        return _get_data_node_count()

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
        # Thread-safe: protect access to last_print_time and report state
        with self._process_lock:
            time_taken = round(time.time() - self.last_print_time, 1)
            # Print report every 2 minutes
            if time_taken > 120:
                self._update_report()
                self.last_print_time = time.time()
                logger.info(f"\n{self.report.as_string()}")

    def process_batch(self, results: Iterable[dict]) -> None:
        """Process a batch of results. Thread-safe for parallel processing."""
        for doc in results:
            self._print_report()
            row = ElasticGraphRow.from_elastic_doc(doc["_source"])
            # Thread-safe updates to shared state
            with self._process_lock:
                self.report.edges_scanned += 1
                if (
                    row.source_urn in self.valid_urns
                    and row.destination_urn in self.valid_urns
                ):
                    self.upstream_counts[row.source_urn] += 1
                    self.downstream_counts[row.destination_urn] += 1

    def _process_slice(
        self,
        server: OpenSearch,
        index: str,
        query: dict,
        slice_id: int,
        num_slices: int,
        batch_size: int,
        scroll: str = "10m",
    ) -> None:
        """Process a single slice in parallel. This method is thread-safe."""
        # Create a copy of the base query for this slice
        slice_query = {**query}

        # Add slice parameter for parallel processing
        # Each slice corresponds to a shard for optimal performance
        slice_query.update({"slice": {"id": slice_id, "max": num_slices}})
        logger.info(f"Processing slice {slice_id + 1} of {num_slices} in thread")

        scroll_id = None
        try:
            # Initial search with scroll
            results = self._search_with_retry(
                server, index, slice_query, batch_size, scroll=scroll
            )
            scroll_id = results.get("_scroll_id")
            self.process_batch(results["hits"]["hits"])

            # Process all pages for this slice using scroll
            while True:
                if len(results["hits"]["hits"]) < batch_size:
                    break
                if not scroll_id:
                    break
                results = self._scroll_with_retry(server, scroll_id, scroll=scroll)
                scroll_id = results.get("_scroll_id")
                self.process_batch(results["hits"]["hits"])
        finally:
            # Clear scroll context - ensure cleanup even if exceptions occur
            if scroll_id:
                try:
                    self._clear_scroll_with_retry(server, scroll_id)
                except Exception as e:
                    logger.warning(
                        f"Failed to clear scroll for slice {slice_id + 1}: {e}"
                    )

        logger.info(f"Completed processing slice {slice_id + 1} of {num_slices}")

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
            }
        }

        index = f"{self.config.search_index.index_prefix}graph_service_v1"

        # Get the number of data nodes and calculate slices as datanodes * 2
        # This will be used to set the slice count for parallel search processing
        data_node_count = self._get_data_node_count_with_retry(server)
        num_slices = data_node_count * 2

        # Add slicing for parallel search processing
        # Slicing divides the search into multiple slices that can be processed in parallel
        # Each slice processes a subset of the data independently

        batch_size = self.config.extract_batch_size
        scroll = "10m"
        with self.report.new_stage("Extract lineage features"):
            try:
                # Only use slicing if we have more than 1 slice (max must be > 1)
                if num_slices > 1:
                    logger.info(
                        f"Using {num_slices} slices for parallel processing with threading "
                        f"(based on {data_node_count} data node(s) * 2)"
                    )
                    # Process slices in parallel using ThreadPoolExecutor
                    # Each slice runs in its own thread, truly parallelizing the OpenSearch queries
                    with ThreadPoolExecutor(max_workers=num_slices) as executor:
                        # Submit all slice processing tasks
                        future_to_slice = {
                            executor.submit(
                                self._process_slice,
                                server,
                                index,
                                query,
                                slice_id,
                                num_slices,
                                batch_size,
                                scroll,
                            ): slice_id
                            for slice_id in range(num_slices)
                        }

                        # Wait for all slices to complete and handle any exceptions
                        for future in as_completed(future_to_slice):
                            slice_id = future_to_slice[future]
                            try:
                                future.result()  # This will raise any exception that occurred
                            except Exception as exc:
                                logger.error(
                                    f"Slice {slice_id + 1} generated an exception: {exc}"
                                )
                                raise
                else:
                    # Single slice - no slicing needed
                    logger.info(
                        "Processing without slicing (single slice or no data nodes)"
                    )
                    scroll_id = None
                    try:
                        # Initial search with scroll
                        results = self._search_with_retry(
                            server, index, query, batch_size, scroll=scroll
                        )
                        scroll_id = results.get("_scroll_id")
                        self.process_batch(results["hits"]["hits"])

                        # Process all pages using scroll
                        while True:
                            if len(results["hits"]["hits"]) < batch_size:
                                break
                            if not scroll_id:
                                break
                            results = self._scroll_with_retry(
                                server, scroll_id, scroll=scroll
                            )
                            scroll_id = results.get("_scroll_id")
                            self.process_batch(results["hits"]["hits"])
                    finally:
                        # Clear scroll context - ensure cleanup even if exceptions occur
                        if scroll_id:
                            try:
                                self._clear_scroll_with_retry(server, scroll_id)
                            except Exception as cleanup_error:
                                logger.warning(
                                    f"Failed to clear scroll after error: {cleanup_error}"
                                )
            except Exception as e:
                logger.error(f"Error during lineage extraction: {e}")
                self.report.report_failure(
                    title="Lineage extraction failed",
                    message="Failed to extract lineage features from Elasticsearch",
                    context=f"Error: {str(e)}",
                    exc=e,
                )
                raise
        self._update_report()

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
