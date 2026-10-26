"""Dataplex lineage extraction module."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

from google.api_core import exceptions as google_exceptions
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from google.cloud.datacatalog_lineage import LineageClient
    from google.cloud.datacatalog_lineage.types import Link

from google.cloud.datacatalog_lineage import EntityReference, SearchLinksRequest
from google.oauth2 import service_account

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntryDataTuple,
    PerLocationRateLimiter,
)
from datahub.ingestion.source.dataplex.dataplex_ids import (
    DATAPROC_METASTORE_TABLE_FQN_REGEX,
    build_gcs_bucket_urn,
    build_hive_table_urn,
    parse_gcs_bucket_fqn,
    parse_hive_metastore_fqn,
    parse_pubsub_subscription_fqn,
    parse_with_regex,
)
from datahub.ingestion.source.dataplex.dataplex_mappers import (
    DATAPROC_METASTORE_TABLE_ENTRY_TYPE,
    dataproc_metastore_table_urn,
    dataset_urn_from_fqn_only,
    is_lineage_supported,
    pubsub_topic_urn,
)
from datahub.ingestion.source.dataplex.dataplex_pubsub import (
    PubSubSubscriptionResolver,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

# Naive upper bound on in-flight futures submitted to the thread pool at once.
# Prevents O(N) memory growth when there are thousands of entries.
# TODO: replace with proper backpressure (e.g. bounded queue / semaphore).
WORKERS_BATCH_SIZE = 200

# The default is 10, and the pager silently fetches every page. 100 is the max.
SEARCH_LINKS_PAGE_SIZE = 100


# Sentinel distinguishing "key absent" from "key present but ambiguous (None)"
# in the Dataproc Metastore (database, table) -> URN indexes.
class _Unset:
    pass


_UNSET = _Unset()

_IndexKeyT = TypeVar("_IndexKeyT")

FAILED_PARENTS_KEY = "failed_parents"


def _index_unique_urn(
    index: Dict[_IndexKeyT, Optional[str]], key: _IndexKeyT, urn: str
) -> None:
    """Store ``urn`` under ``key``; a key claimed by two URNs becomes None."""
    existing = index.get(key, _UNSET)
    if isinstance(existing, _Unset):
        index[key] = urn
    elif existing is not None and existing != urn:
        index[key] = None


def build_lineage_parent(project_id: str, location: str) -> str:
    """Build the Data Lineage API parent for an explicit project/location pair."""
    return f"projects/{project_id}/locations/{location}"


@dataclass(order=True, eq=True, frozen=True)
class LineageEdge:
    """
    Represents a lineage edge between two entries.

    This dataclass uses frozen=True, eq=True, and order=True because:
    - frozen=True: Makes instances immutable and hashable, allowing them to be stored in sets
    - eq=True: Enables equality comparison to detect and prevent duplicate edges in sets
    - order=True: Provides consistent ordering for deterministic iteration and sorting

    LineageEdge instances are stored in sets during per-entry processing, requiring
    immutability and hashability.

    Attributes:
        upstream_datahub_urn: The upstream dataset URN normalized for DataHub
        audit_stamp: When this lineage was observed
        lineage_type: Type of lineage (TRANSFORMED, COPY, etc.)
    """

    upstream_datahub_urn: str
    audit_stamp: datetime
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED


@dataclass
class LocationScanStats:
    """Per project/location scan counters for lineage API lookups."""

    calls: int = 0
    hits: int = 0
    empty: int = 0
    errors: int = 0


@dataclass
class DataplexLineageReport(Report):
    """Lineage-specific observability for Dataplex ingestion."""

    num_lineage_relationships_created: int = 0
    lineage_relationships_created_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    num_lineage_entries_processed: int = 0
    lineage_entries_processed_samples: LossyList[str] = field(default_factory=LossyList)
    num_lineage_entries_scanned: int = 0
    lineage_entries_scanned_samples: LossyList[str] = field(default_factory=LossyList)
    num_lineage_entries_without_lineage: int = 0
    lineage_entries_without_lineage_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    num_lineage_entries_skipped_unsupported_type: int = 0
    lineage_entries_skipped_unsupported_type_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    num_lineage_upstream_fqns_skipped: int = 0
    lineage_upstream_fqns_skipped_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    num_lineage_upstream_links_found: int = 0
    lineage_upstream_links_found_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    num_lineage_downstream_links_found: int = 0
    lineage_downstream_links_found_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    num_lineage_edges_added: int = 0
    lineage_edges_added_samples: LossyList[str] = field(default_factory=LossyList)
    num_lineage_entries_failed: int = 0
    lineage_entries_failed_samples: LossyList[str] = field(default_factory=LossyList)
    # hive_metastore FQN resolution.
    num_hive_metastore_fqns_resolved: int = 0
    num_hive_metastore_fqns_unresolved: int = 0
    hive_metastore_fqns_unresolved_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    # (database, table) pairs found under more than one service in this run.
    num_hive_metastore_fqns_ambiguous: int = 0
    hive_metastore_fqns_ambiguous_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    # Uncatalogued hive tables emitted as lineage-only 'hive' platform nodes.
    num_hive_metastore_fallback_nodes: int = 0
    hive_metastore_fallback_node_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    # GCS bucket -> table edges derived from storage aspects.
    num_storage_lineage_edges_added: int = 0
    storage_lineage_edges_added_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    num_storage_lineage_missing: int = 0
    storage_lineage_missing_samples: LossyList[str] = field(default_factory=LossyList)
    num_storage_lineage_withheld: int = 0
    storage_lineage_withheld_samples: LossyList[str] = field(default_factory=LossyList)
    # pubsub:subscription: upstreams. Resolved counts entry lookups that built a
    # topic URN; unresolved counts distinct subscriptions.
    num_pubsub_subscriptions_resolved: int = 0
    num_pubsub_subscriptions_unresolved: int = 0
    pubsub_subscriptions_unresolved_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    num_pubsub_subscription_cache_hits: int = 0
    lineage_api: dict[str, tuple[int, float]] = field(default_factory=dict)
    scan_stats_by_project_location_pair: dict[tuple[str, str], LocationScanStats] = (
        field(default_factory=dict)
    )

    def __post_init__(self) -> None:
        # Lock protecting all mutable fields when report methods are called from
        # parallel worker threads in get_lineage_workunits_parallel.
        self._lock: threading.Lock = threading.Lock()

    def report_lineage_api_call(self, api_name: str, elapsed_seconds: float) -> None:
        """Accumulate per-API call count and total latency in seconds."""
        with self._lock:
            num_calls, total_time_secs = self.lineage_api.get(api_name, (0, 0.0))
            self.lineage_api[api_name] = (
                num_calls + 1,
                total_time_secs + elapsed_seconds,
            )

    def _lineage_scan_stats_for(
        self, project_id: str, location: str
    ) -> LocationScanStats:
        # Caller must hold self._lock.
        key = (project_id, location)
        if key not in self.scan_stats_by_project_location_pair:
            self.scan_stats_by_project_location_pair[key] = LocationScanStats()
        return self.scan_stats_by_project_location_pair[key]

    def report_lineage_scan_call(self, project_id: str, location: str) -> None:
        with self._lock:
            self._lineage_scan_stats_for(project_id, location).calls += 1

    def report_lineage_scan_hit(self, project_id: str, location: str) -> None:
        with self._lock:
            self._lineage_scan_stats_for(project_id, location).hits += 1

    def report_lineage_scan_empty(self, project_id: str, location: str) -> None:
        with self._lock:
            self._lineage_scan_stats_for(project_id, location).empty += 1

    def report_lineage_scan_error(self, project_id: str, location: str) -> None:
        with self._lock:
            self._lineage_scan_stats_for(project_id, location).errors += 1

    def report_lineage_relationship_created(self, relationship: str) -> None:
        with self._lock:
            self.num_lineage_relationships_created += 1
            self.lineage_relationships_created_samples.append(relationship)
        logger.debug(f"Lineage relationship created: {relationship}")

    def report_lineage_entry_processed(self, entry_name: str) -> None:
        with self._lock:
            self.num_lineage_entries_processed += 1
            self.lineage_entries_processed_samples.append(entry_name)
        logger.debug(f"Lineage entry processed: {entry_name}")

    def report_lineage_entry_scanned(self, entry_name: str) -> None:
        with self._lock:
            self.num_lineage_entries_scanned += 1
            self.lineage_entries_scanned_samples.append(entry_name)
        logger.debug(f"Lineage entry has links: {entry_name}")

    def report_lineage_entry_without_lineage(
        self, entry_name: str, reason: str
    ) -> None:
        with self._lock:
            self.num_lineage_entries_without_lineage += 1
            self.lineage_entries_without_lineage_samples.append(
                f"entry={entry_name}, reason={reason}"
            )
        logger.debug(f"Lineage missing for entry {entry_name} (reason={reason})")

    def report_lineage_entry_skipped_unsupported_type(
        self, entry_name: str, entry_type: str
    ) -> None:
        with self._lock:
            self.num_lineage_entries_skipped_unsupported_type += 1
            self.lineage_entries_skipped_unsupported_type_samples.append(
                f"entry={entry_name}, entry_type={entry_type}"
            )
        logger.debug(
            f"Lineage skipped unsupported entry type for {entry_name}: {entry_type}"
        )

    def report_lineage_upstream_fqn_skipped(
        self, entry_name: str, upstream_fqn: str
    ) -> None:
        with self._lock:
            self.num_lineage_upstream_fqns_skipped += 1
            self.lineage_upstream_fqns_skipped_samples.append(
                f"entry={entry_name}, upstream_fqn={upstream_fqn}"
            )
        logger.debug(
            f"Lineage upstream FQN skipped for {entry_name}: upstream_fqn={upstream_fqn}"
        )

    def report_lineage_upstream_links_found(self, entry_name: str, count: int) -> None:
        if count <= 0:
            return
        with self._lock:
            self.num_lineage_upstream_links_found += count
            self.lineage_upstream_links_found_samples.append(
                f"entry={entry_name}, count={count}"
            )
        logger.debug(f"Lineage upstream links observed for {entry_name}: {count}")

    def report_lineage_downstream_links_found(
        self, entry_name: str, count: int
    ) -> None:
        if count <= 0:
            return
        with self._lock:
            self.num_lineage_downstream_links_found += count
            self.lineage_downstream_links_found_samples.append(
                f"entry={entry_name}, count={count}"
            )
        logger.debug(f"Lineage downstream links observed for {entry_name}: {count}")

    def report_lineage_edge_added(
        self, downstream_dataset_id: str, upstream_dataset_urn: str
    ) -> None:
        with self._lock:
            self.num_lineage_edges_added += 1
            self.lineage_edges_added_samples.append(
                f"{downstream_dataset_id}<-{upstream_dataset_urn}"
            )
        logger.debug(
            f"Lineage edge added: {downstream_dataset_id} <- {upstream_dataset_urn}"
        )

    def report_lineage_entry_failed(self, entry_name: str, stage: str) -> None:
        with self._lock:
            self.num_lineage_entries_failed += 1
            self.lineage_entries_failed_samples.append(
                f"entry={entry_name}, stage={stage}"
            )
        logger.debug(f"Lineage entry failed: {entry_name} (stage={stage})")

    def report_hive_metastore_fqn_resolved(self) -> None:
        with self._lock:
            self.num_hive_metastore_fqns_resolved += 1

    def report_hive_metastore_fqn_unresolved(self, upstream_fqn: str) -> None:
        with self._lock:
            self.num_hive_metastore_fqns_unresolved += 1
            self.hive_metastore_fqns_unresolved_samples.append(upstream_fqn)

    def report_hive_metastore_fqn_ambiguous(self, upstream_fqn: str) -> None:
        with self._lock:
            self.num_hive_metastore_fqns_ambiguous += 1
            self.hive_metastore_fqns_ambiguous_samples.append(upstream_fqn)

    def report_hive_metastore_fallback_node(self, table_name: str) -> None:
        with self._lock:
            self.num_hive_metastore_fallback_nodes += 1
            self.hive_metastore_fallback_node_samples.append(table_name)

    def report_storage_lineage_edge_added(
        self, downstream_dataset_id: str, upstream_dataset_urn: str
    ) -> None:
        with self._lock:
            self.num_storage_lineage_edges_added += 1
            self.storage_lineage_edges_added_samples.append(
                f"{downstream_dataset_id}<-{upstream_dataset_urn}"
            )

    def report_storage_lineage_missing(self, entry_name: str) -> None:
        with self._lock:
            self.num_storage_lineage_missing += 1
            self.storage_lineage_missing_samples.append(entry_name)

    def report_storage_lineage_withheld(self, entry_name: str) -> None:
        with self._lock:
            self.num_storage_lineage_withheld += 1
            self.storage_lineage_withheld_samples.append(entry_name)

    def report_pubsub_subscription_resolved(self) -> None:
        with self._lock:
            self.num_pubsub_subscriptions_resolved += 1

    def report_pubsub_subscription_unresolved(self, context: str) -> None:
        with self._lock:
            self.num_pubsub_subscriptions_unresolved += 1
            self.pubsub_subscriptions_unresolved_samples.append(context)

    def report_pubsub_subscription_cache_hit(self) -> None:
        with self._lock:
            self.num_pubsub_subscription_cache_hits += 1


class _EntryLineageEdges:
    """Per-entry accumulator keeping exactly one edge per upstream URN."""

    def __init__(
        self, report: DataplexLineageReport, downstream_dataset_id: str
    ) -> None:
        self._report = report
        self._downstream_dataset_id = downstream_dataset_id
        self.edges_by_urn: Dict[str, LineageEdge] = {}

    def seed(self, edge: LineageEdge) -> None:
        """Pre-register an edge built outside the Data Lineage API."""
        self.edges_by_urn[edge.upstream_datahub_urn] = edge

    def add(self, upstream_dataset_urn: str) -> None:
        if upstream_dataset_urn in self.edges_by_urn:
            return
        self.edges_by_urn[upstream_dataset_urn] = LineageEdge(
            upstream_datahub_urn=upstream_dataset_urn,
            audit_stamp=datetime.now(timezone.utc),
            lineage_type=DatasetLineageTypeClass.TRANSFORMED,
        )
        self._report.report_lineage_edge_added(
            downstream_dataset_id=self._downstream_dataset_id,
            upstream_dataset_urn=upstream_dataset_urn,
        )
        logger.debug(
            "  Added lineage edge: %s <- %s",
            self._downstream_dataset_id,
            upstream_dataset_urn,
        )


class DataplexLineageExtractor:
    """
    Extracts lineage information from Google Dataplex using the Data Lineage API.

    This class queries the Dataplex Lineage API to discover upstream
    relationships between entries and generates DataHub lineage metadata.
    """

    def __init__(
        self,
        config: DataplexConfig,
        report: DataplexLineageReport,
        source_report: SourceReport,
        lineage_client: Optional[LineageClient] = None,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
        credentials: Optional[service_account.Credentials] = None,
    ):
        """
        Initialize the lineage extractor.

        Args:
            config: Dataplex source configuration
            report: Lineage report for lineage-specific counters
            source_report: Source report for warning/failure emission
            lineage_client: Optional pre-configured LineageClient
            redundant_run_skip_handler: Optional redundant lineage run skip handler
            credentials: Optional GCP credentials for the Pub/Sub client
        """
        self.config = config
        self.report = report
        self.source_report = source_report
        self.lineage_client = lineage_client
        # TODO: Use redundant_run_skip_handler to short-circuit lineage calls when stateful
        # lineage ingestion determines this run is redundant.
        self.redundant_run_skip_handler = redundant_run_skip_handler
        # Shared by all lineage workers.
        self._rate_limiter = PerLocationRateLimiter(config.lineage_max_calls_per_minute)
        # (database, table) -> URN; None marks an ambiguous key.
        self._dpms_urn_by_db_table: Dict[Tuple[str, str], Optional[str]] = {}
        self._dpms_urn_by_db_table_casefold: Dict[Tuple[str, str], Optional[str]] = {}
        # Keyed by the dpms_hive_metastore_service form plus the casefolded pair.
        self._dpms_urn_by_service_table: Dict[Tuple[str, str, str], Optional[str]] = {}
        # Subscription -> backing-topic resolution for Dataflow-reported
        # pubsub:subscription: upstream FQNs. None means the feature is off.
        self._pubsub_resolver: Optional[PubSubSubscriptionResolver] = None
        if config.resolve_pubsub_subscriptions:
            self._pubsub_resolver = PubSubSubscriptionResolver(
                report=report, source_report=source_report, credentials=credentials
            )

    def get_lineage_for_entry(
        self,
        entry: EntryDataTuple,
        active_lineage_project_location_pairs: list[tuple[str, str]],
    ) -> Optional[Dict[str, list]]:
        """
        Get lineage information for a specific Dataplex entry with automatic retries.

        This method uses tenacity to automatically retry transient errors (timeouts, rate limits, etc.)
        with exponential backoff. After retries are exhausted, logs a warning and continues.

        Args:
            entry: Dataplex entry metadata used as the lineage lookup target.
            active_lineage_project_location_pairs: Explicit ``(project_id, location)``
                parents to query in the Lineage API.

        Returns:
            On success, returns a dictionary with keys:
            - ``"upstream"``: list of upstream fully-qualified names discovered
              from target-link search across scanned parents.
            - ``"downstream"``: always an empty list (kept for backward-compatible
              return shape).
            - ``"failed_parents"``: parents whose lookup raised.
            Returns ``None`` when lineage is disabled/unavailable or when lookup
            fails after retries/exception handling.
        """
        if not self.config.include_lineage or not self.lineage_client:
            return None

        try:
            fully_qualified_name = entry.dataplex_entry_fqn
            lineage_data: dict[str, list[str]] = {
                "upstream": [],
                "downstream": [],
                FAILED_PARENTS_KEY: [],
            }
            hit_parents: list[str] = []
            empty_parents: list[str] = []
            # Query only target links (upstream lineage) across configured project/location
            # matrix so cross-project lineage edges can be discovered.
            scan_pairs = active_lineage_project_location_pairs
            for lineage_project_id, lineage_location in scan_pairs:
                parent = build_lineage_parent(lineage_project_id, lineage_location)
                self.report.report_lineage_scan_call(
                    lineage_project_id, lineage_location
                )
                try:
                    with PerfTimer() as timer:
                        upstream_links = self._search_links_by_target(
                            parent, fully_qualified_name
                        )
                        self.report.report_lineage_api_call(
                            "search_links_by_target", timer.elapsed_seconds()
                        )
                except Exception as parent_error:
                    self.report.report_lineage_scan_error(
                        lineage_project_id, lineage_location
                    )
                    self.source_report.warning(
                        "Failed to query Dataplex lineage for a project/location parent. Continuing with remaining parents.",
                        context=(
                            f"parent={parent}, "
                            f"dataplex_entry_name={entry.dataplex_entry_name}, "
                            f"datahub_dataset_name={entry.datahub_dataset_name}, "
                            f"entry_type={entry.dataplex_entry_type_short_name}"
                        ),
                        exc=parent_error,
                    )
                    lineage_data[FAILED_PARENTS_KEY].append(parent)
                    continue

                if upstream_links:
                    hit_parents.append(parent)
                    self.report.report_lineage_scan_hit(
                        lineage_project_id, lineage_location
                    )
                else:
                    empty_parents.append(parent)
                    self.report.report_lineage_scan_empty(
                        lineage_project_id, lineage_location
                    )

                for link in upstream_links:
                    if link.source and link.source.fully_qualified_name:
                        lineage_data["upstream"].append(
                            link.source.fully_qualified_name
                        )

            if not hit_parents and not empty_parents:
                self.report.report_lineage_entry_failed(
                    entry_name=entry.dataplex_entry_name,
                    stage="all_parents_failed",
                )
                return None

            logger.debug(
                "Lineage lookup summary for entry=%s fqn=%s: hit_parents=%s empty_parents=%s",
                entry.dataplex_entry_name,
                fully_qualified_name,
                hit_parents,
                empty_parents,
            )

            if lineage_data["upstream"]:
                logger.debug(
                    f"Found lineage for {entry.dataplex_entry_short_name}: "
                    f"{len(lineage_data['upstream'])} upstream, 0 downstream"
                )
                self.report.report_lineage_entry_scanned(entry.dataplex_entry_name)

            return lineage_data

        except Exception as e:
            # After retries are exhausted, report structured warning and continue
            self.report.report_lineage_entry_failed(
                entry_name=entry.dataplex_entry_name,
                stage="get_lineage_for_entry",
            )
            self.source_report.warning(
                "Failed to get lineage for entry after retries. Continuing with other entries.",
                context=(
                    f"dataplex_entry_name={entry.dataplex_entry_name}, "
                    f"datahub_dataset_name={entry.datahub_dataset_name}, "
                    f"entry_type={entry.dataplex_entry_type_short_name}"
                ),
                exc=e,
            )
            return None

    def _get_retry_decorator(self):
        """Create a retry decorator with config-based parameters."""
        return retry(
            retry=retry_if_exception_type(
                (
                    google_exceptions.DeadlineExceeded,
                    google_exceptions.ServiceUnavailable,
                    google_exceptions.TooManyRequests,
                    google_exceptions.InternalServerError,
                )
            ),
            wait=wait_exponential(
                multiplier=self.config.lineage_retry_backoff_multiplier,
                min=2,
                max=self.config.lineage_retry_max_wait_seconds,
            ),
            stop=stop_after_attempt(self.config.lineage_max_retries),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    def _search_links_by_target_impl(
        self, parent: str, fully_qualified_name: str
    ) -> list["Link"]:
        """
        Implementation of searching for lineage links where the entry is a target (to find upstream).
        This method is wrapped with retry logic in _search_links_by_target.

        Raises:
            RuntimeError: If lineage client is not initialized
        """
        if self.lineage_client is None:
            raise RuntimeError("Lineage client is not initialized")
        logger.debug(f"Searching upstream lineage for FQN: {fully_qualified_name}")
        target = EntityReference(fully_qualified_name=fully_qualified_name)
        request = SearchLinksRequest(
            parent=parent, target=target, page_size=SEARCH_LINKS_PAGE_SIZE
        )
        # Inside the retry loop: every attempt spends quota.
        self._rate_limiter.acquire(parent)
        # Convert pager to list - this automatically handles pagination
        results = list(self.lineage_client.search_links(request=request))
        logger.debug(
            f"Found {len(results)} upstream lineage link(s) for {fully_qualified_name}"
        )
        return results

    def _search_links_by_target(
        self, parent: str, fully_qualified_name: str
    ) -> list["Link"]:
        """
        Search for lineage links where the entry is a target (to find upstream).

        Applies configurable retry logic with exponential backoff for transient errors.

        Args:
            parent: Parent resource path (projects/{project}/locations/{location})
            fully_qualified_name: FQN of the entry

        Returns:
            List of Link objects (all pages automatically retrieved)

        Raises:
            Exception: If the lineage API call fails after all retries
        """
        # Apply retry decorator dynamically based on config
        retry_decorator = self._get_retry_decorator()
        retrying_func = retry_decorator(self._search_links_by_target_impl)
        return retrying_func(parent, fully_qualified_name)

    def register_dpms_tables(self, entries: Iterable[EntryDataTuple]) -> None:
        """Index this run's Dataproc Metastore tables by ``(database, table)``.

        Each pair is indexed across services and per service. A key claimed by
        two different tables is marked ambiguous (None).
        """
        for entry in entries:
            if (
                entry.dataplex_entry_type_short_name
                != DATAPROC_METASTORE_TABLE_ENTRY_TYPE
            ):
                continue
            identity_fields = parse_with_regex(
                DATAPROC_METASTORE_TABLE_FQN_REGEX, entry.dataplex_entry_fqn
            )
            if identity_fields is None:
                continue
            database_id = identity_fields["database_id"]
            table_id = identity_fields["table_id"]
            folded = (database_id.casefold(), table_id.casefold())
            service = ".".join(
                identity_fields[part]
                for part in ("project_id", "location", "service_id")
            )
            urn = entry.datahub_dataset_urn
            _index_unique_urn(self._dpms_urn_by_db_table, (database_id, table_id), urn)
            _index_unique_urn(self._dpms_urn_by_db_table_casefold, folded, urn)
            _index_unique_urn(
                self._dpms_urn_by_service_table, (service, folded[0], folded[1]), urn
            )

    def close(self) -> None:
        """Release per-run resources."""
        if self._pubsub_resolver is not None:
            self._pubsub_resolver.close()

    def _resolve_gcs_fqn(self, upstream_fqn: str) -> Optional[str]:
        """Bucket URN for a ``gcs:`` FQN."""
        bucket_name = parse_gcs_bucket_fqn(upstream_fqn)
        if bucket_name is None:
            return None
        return build_gcs_bucket_urn(bucket_name, self.config.env)

    def _match_dpms_table(
        self, database_id: str, table_id: str
    ) -> Union[str, _Unset, None]:
        """This run's table URN for the pair: None if ambiguous, _UNSET if absent."""
        hit = self._dpms_urn_by_db_table.get((database_id, table_id), _UNSET)
        if isinstance(hit, _Unset):
            hit = self._dpms_urn_by_db_table_casefold.get(
                (database_id.casefold(), table_id.casefold()), _UNSET
            )
        return hit

    def _resolve_hive_metastore_fqn(self, upstream_fqn: str) -> Optional[str]:
        """URN for a ``hive_metastore:`` FQN, or None.

        Tries this run's tables, then ``dpms_hive_metastore_service``, then a hive node.
        """
        parsed = parse_hive_metastore_fqn(upstream_fqn)
        if parsed is None:
            return None
        database_id, table_id = parsed.database_id, parsed.table_id

        hit = self._match_dpms_table(database_id, table_id)
        if isinstance(hit, str):
            self.report.report_hive_metastore_fqn_resolved()
            return hit
        if hit is None:
            self.report.report_hive_metastore_fqn_ambiguous(upstream_fqn)

        fallback = self.config.dpms_hive_metastore_service
        if fallback:
            urn = self._dpms_urn_by_service_table.get(
                (fallback, database_id.casefold(), table_id.casefold())
            )
            if urn is None:
                # The config validator guarantees three dotted parts.
                project_id, location, service_id = fallback.split(".")
                urn = dataproc_metastore_table_urn(
                    project_id=project_id,
                    location=location,
                    service_id=service_id,
                    database_id=database_id,
                    table_id=table_id,
                    env=self.config.env,
                )
            if urn is not None:
                self.report.report_hive_metastore_fqn_resolved()
                return urn

        if self.config.include_hive_metastore_nodes:
            self.report.report_hive_metastore_fallback_node(f"{database_id}.{table_id}")
            return build_hive_table_urn(database_id, table_id, env=self.config.env)
        self.report.report_hive_metastore_fqn_unresolved(upstream_fqn)
        return None

    def _resolve_pubsub_subscription_fqn(self, upstream_fqn: str) -> Optional[str]:
        """Backing topic's URN for a subscription FQN, or None."""
        parsed = parse_pubsub_subscription_fqn(upstream_fqn)
        if parsed is None or self._pubsub_resolver is None:
            return None
        topic = self._pubsub_resolver.resolve_topic(
            parsed.project_id, parsed.subscription_id
        )
        if topic is None:
            return None
        urn = pubsub_topic_urn(
            project_id=topic.project_id, topic_id=topic.topic_id, env=self.config.env
        )
        if urn is not None:
            self.report.report_pubsub_subscription_resolved()
        return urn

    def _storage_lineage_edge(
        self, entry: EntryDataTuple, withhold: bool = False
    ) -> Optional[LineageEdge]:
        """Bucket -> table edge from a Dataproc Metastore table's storage aspect."""
        if not self.config.include_storage_lineage:
            return None
        if entry.dataplex_entry_type_short_name != DATAPROC_METASTORE_TABLE_ENTRY_TYPE:
            return None
        if not entry.storage_gcs_bucket:
            self.report.report_storage_lineage_missing(entry.dataplex_entry_name)
            return None
        if withhold:
            self.report.report_storage_lineage_withheld(entry.dataplex_entry_name)
            return None

        bucket_urn = build_gcs_bucket_urn(entry.storage_gcs_bucket, self.config.env)
        self.report.report_storage_lineage_edge_added(
            downstream_dataset_id=entry.datahub_dataset_name,
            upstream_dataset_urn=bucket_urn,
        )
        return LineageEdge(
            upstream_datahub_urn=bucket_urn,
            audit_stamp=datetime.now(timezone.utc),
            lineage_type=DatasetLineageTypeClass.TRANSFORMED,
        )

    def _extract_lineage_edges_for_entry(
        self, entry: EntryDataTuple, lineage_data: Optional[dict[str, list[str]]]
    ) -> set[LineageEdge]:
        """Convert raw lookup payload into normalized DataHub lineage edges.

        Args:
            entry: Downstream Dataplex entry currently being processed.
            lineage_data: Result payload from ``get_lineage_for_entry``. Expected
                shape is ``{"upstream": list[str], "downstream": list[str],
                "failed_parents": list[str]}``. Only ``upstream`` values become
                edges. ``None`` produces no edges. When no upstream resolves to
                an edge, only the storage edge is returned, if
                ``include_storage_lineage`` is on and no parent failed.

        Returns:
            A deduplicated set of ``LineageEdge`` records normalized to DataHub
            upstream dataset URNs.
        """
        self.report.report_lineage_entry_processed(entry.dataplex_entry_name)
        if not lineage_data:
            # The lookup failed; an empty aspect would wipe the stored upstreams.
            self.report.report_lineage_entry_without_lineage(
                entry_name=entry.dataplex_entry_name,
                reason="lineage_lookup_failed_or_unavailable",
            )
            return set()

        parent_failed = bool(lineage_data.get(FAILED_PARENTS_KEY))
        upstream_count = len(lineage_data.get("upstream", []))
        self.report.report_lineage_upstream_links_found(
            entry_name=entry.dataplex_entry_name,
            count=upstream_count,
        )
        if upstream_count == 0:
            storage_edges = self._storage_only_edges(entry, parent_failed)
            if not storage_edges:
                self.report.report_lineage_entry_without_lineage(
                    entry_name=entry.dataplex_entry_name,
                    reason="empty_upstream",
                )
            return storage_edges

        if not is_lineage_supported(entry.dataplex_entry_type_short_name):
            self.report.report_lineage_entry_skipped_unsupported_type(
                entry_name=entry.dataplex_entry_name,
                entry_type=entry.dataplex_entry_type_short_name,
            )
            return self._storage_only_edges(entry, parent_failed)

        # Every returning parent repeats the same FQN; resolve each once.
        resolved_fqns: Dict[str, Optional[str]] = {}
        upstream_urns: List[str] = []
        for upstream_fqn in lineage_data.get("upstream", []):
            upstream_urn = self._resolve_upstream_fqn(upstream_fqn, resolved_fqns)
            if upstream_urn is None:
                self._report_unresolved_upstream(entry, upstream_fqn)
                continue
            upstream_urns.append(upstream_urn)
        if not upstream_urns:
            return self._storage_only_edges(entry, parent_failed)

        edges = _EntryLineageEdges(
            report=self.report,
            downstream_dataset_id=entry.datahub_dataset_name,
        )
        storage_edge = self._storage_lineage_edge(entry)
        if storage_edge is not None:
            # Seeded so an API link to the same bucket merges into it.
            edges.seed(storage_edge)
        for upstream_urn in upstream_urns:
            edges.add(upstream_urn)
        return set(edges.edges_by_urn.values())

    def _storage_only_edges(
        self, entry: EntryDataTuple, parent_failed: bool
    ) -> set[LineageEdge]:
        """The storage edge alone, for a lookup where no API link resolved."""
        # A failed parent may hold this table's links, which a storage-only
        # upstreamLineage would replace.
        storage_edge = self._storage_lineage_edge(entry, withhold=parent_failed)
        return {storage_edge} if storage_edge is not None else set()

    def _resolve_upstream_fqn(
        self,
        upstream_fqn: str,
        resolved_fqns: Dict[str, Optional[str]],
    ) -> Optional[str]:
        """Normalize one upstream FQN to a dataset URN, memoized per entry."""
        if upstream_fqn in resolved_fqns:
            return resolved_fqns[upstream_fqn]
        # Upstream FQN may be cross-platform (e.g. pubsub->bigquery), so
        # normalize to DataHub URN using a mapping lookup driven only by FQN shape.
        resolved = dataset_urn_from_fqn_only(
            fully_qualified_name=upstream_fqn,
            env=self.config.env,
        )
        if resolved is None:
            # Shapes the Data Lineage API reports that are not Dataplex entry
            # types, and so are deliberately absent from the mapper registry.
            resolved = self._resolve_gcs_fqn(upstream_fqn)
        if resolved is None:
            # Spark-reported hive_metastore FQNs carry no project or service;
            # resolve them against this run's catalog entries.
            resolved = self._resolve_hive_metastore_fqn(upstream_fqn)
        if resolved is None:
            # Dataflow-reported subscriptions resolve to their backing topic so
            # the edge joins the catalogued topic entity.
            resolved = self._resolve_pubsub_subscription_fqn(upstream_fqn)
        resolved_fqns[upstream_fqn] = resolved
        return resolved

    def _report_unresolved_upstream(
        self, entry: EntryDataTuple, upstream_fqn: str
    ) -> None:
        """Count and explain an upstream FQN that produced no edge."""
        self.report.report_lineage_upstream_fqn_skipped(
            entry_name=entry.dataplex_entry_name,
            upstream_fqn=upstream_fqn,
        )
        skip_context = (
            f"dataplex_entry_name={entry.dataplex_entry_name}, "
            f"datahub_dataset_name={entry.datahub_dataset_name}, "
            f"entry_type={entry.dataplex_entry_type_short_name}, "
            f"upstream_fqn={upstream_fqn}"
        )
        hive_ref = parse_hive_metastore_fqn(upstream_fqn)
        if hive_ref is not None:
            # Parsed, but every fallback is off; distinct from a parse failure.
            if self._match_dpms_table(hive_ref.database_id, hive_ref.table_id) is None:
                self.source_report.warning(
                    "hive_metastore upstream matched Dataproc Metastore tables "
                    "under more than one service in this run and the hive-node "
                    "fallback is disabled. Skipping upstream edge. Set "
                    "'dpms_hive_metastore_service' to the service the job uses "
                    "or enable 'include_hive_metastore_nodes'.",
                    title="Dataplex hive_metastore upstream ambiguous",
                    context=skip_context,
                )
            else:
                self.source_report.warning(
                    "hive_metastore upstream matched no Dataproc Metastore table "
                    "in this run and the hive-node fallback is disabled. Skipping "
                    "upstream edge. Set 'dpms_hive_metastore_service' or enable "
                    "'include_hive_metastore_nodes'.",
                    title="Dataplex hive_metastore upstream unmatched",
                    context=skip_context,
                )
        elif parse_pubsub_subscription_fqn(upstream_fqn) is not None:
            self.source_report.warning(
                "Pub/Sub subscription upstream could not be resolved to its "
                "backing topic. Skipping upstream edge. Enable "
                "'resolve_pubsub_subscriptions' and grant "
                "pubsub.subscriptions.get on the subscription's project.",
                title="Dataplex Pub/Sub subscription upstream unresolved",
                context=skip_context,
            )
        else:
            self.source_report.warning(
                "Unable to normalize upstream Dataplex lineage FQN. Skipping upstream edge.",
                title="Dataplex upstream lineage parse failed",
                context=skip_context,
            )

    def _to_upstream_lineage(
        self, dataset_id: str, lineage_edges: set[LineageEdge]
    ) -> Optional[UpstreamLineageClass]:
        """Build UpstreamLineageClass from extracted edges for one dataset.

        Deduplicates edges by ``(upstream_datahub_urn, lineage_type)`` before
        emitting to DataHub. If duplicate keys are present, keeps the earliest
        observed ``audit_stamp`` so emitted lineage remains deterministic.
        """
        unique_upstreams: dict[tuple[str, str], LineageEdge] = {}
        for lineage_edge in lineage_edges:
            dedup_key = (lineage_edge.upstream_datahub_urn, lineage_edge.lineage_type)
            existing = unique_upstreams.get(dedup_key)
            if existing is None or lineage_edge.audit_stamp < existing.audit_stamp:
                unique_upstreams[dedup_key] = lineage_edge

        if not unique_upstreams:
            return None

        upstream_list: list[UpstreamClass] = []
        for lineage_edge in unique_upstreams.values():
            upstream_list.append(
                UpstreamClass(
                    dataset=lineage_edge.upstream_datahub_urn,
                    type=lineage_edge.lineage_type,
                    auditStamp=AuditStampClass(
                        actor="urn:li:corpuser:datahub",
                        time=int(lineage_edge.audit_stamp.timestamp() * 1000),
                    ),
                )
            )
            self.report.report_lineage_relationship_created(
                f"{dataset_id}<-{lineage_edge.upstream_datahub_urn}"
            )
        return UpstreamLineageClass(upstreams=upstream_list)

    def _gen_lineage(
        self,
        dataset_id: str,
        dataset_urn: str,
        upstream_lineage: Optional[UpstreamLineageClass],
    ) -> Iterable[MetadataWorkUnit]:
        if upstream_lineage is None:
            return
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=upstream_lineage
        ).as_workunit()

    def _process_entry_lineage(
        self,
        entry: EntryDataTuple,
        active_lineage_project_location_pairs: list[tuple[str, str]],
    ) -> List[MetadataWorkUnit]:
        """Fetch and build lineage workunits for a single entry.

        Safe to call from parallel worker threads — all shared state mutations
        go through lock-protected report methods, and the lineage client uses
        a thread-safe gRPC channel.

        Returns a list (empty or singleton) of ``MetadataWorkUnit`` objects so
        the result can be collected by the calling thread without a generator.
        """
        lineage_data = self.get_lineage_for_entry(
            entry,
            active_lineage_project_location_pairs=active_lineage_project_location_pairs,
        )
        lineage_edges = self._extract_lineage_edges_for_entry(entry, lineage_data)
        if not lineage_edges:
            return []

        dataset_id = entry.datahub_dataset_name
        dataset_urn = builder.make_dataset_urn_with_platform_instance(
            platform=entry.datahub_platform,
            name=dataset_id,
            platform_instance=None,
            env=self.config.env,
        )
        upstream_lineage = self._to_upstream_lineage(dataset_id, lineage_edges)
        return list(self._gen_lineage(dataset_id, dataset_urn, upstream_lineage))

    def get_lineage_workunits(
        self,
        entry_data: Iterable[EntryDataTuple],
        active_lineage_project_location_pairs: list[tuple[str, str]],
        max_workers: int = 20,
    ) -> Iterable[MetadataWorkUnit]:
        """Extract lineage workunits for all entries using a thread pool.

        Submits one task per entry to a ``ThreadPoolExecutor``.  Each worker
        calls ``_process_entry_lineage`` which internally queries the Dataplex
        Lineage API across all configured ``(project_id, location)`` pairs with
        the configured retry logic.  Results are yielded from the main thread
        as futures complete.

        Args:
            entry_data: Entries to extract lineage for.
            active_lineage_project_location_pairs: Explicit ``(project_id, location)``
                parents to query in the Lineage API.
            max_workers: Maximum number of parallel worker threads.
        """
        if not self.config.include_lineage:
            logger.info("Lineage extraction is disabled")
            return

        # Index Dataproc Metastore tables before any worker resolves a
        # hive_metastore upstream FQN against them.
        entry_data = list(entry_data)
        self.register_dpms_tables(entry_data)

        logger.info("Extracting lineage (parallel, max_workers=%d)", max_workers)

        # Submit entries in bounded batches to cap in-flight futures and prevent
        # O(N) memory growth for large deployments.
        entries_with_lineage = 0
        found_any = False
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            it = iter(entry_data)
            while batch := list(islice(it, WORKERS_BATCH_SIZE)):
                found_any = True
                futures = {
                    executor.submit(
                        self._process_entry_lineage,
                        entry,
                        active_lineage_project_location_pairs,
                    ): entry
                    for entry in batch
                }
                for future in as_completed(futures):
                    entry = futures[future]
                    try:
                        workunits = future.result()
                        if workunits:
                            entries_with_lineage += 1
                        yield from workunits
                    except Exception as exc:
                        self.report.report_lineage_entry_failed(
                            entry_name=entry.dataplex_entry_name,
                            stage="parallel_gen_lineage",
                        )
                        self.source_report.warning(
                            "Failed to generate lineage for entry in parallel worker.",
                            context=(
                                f"dataplex_entry_name={entry.dataplex_entry_name}, "
                                f"datahub_dataset_name={entry.datahub_dataset_name}, "
                                "stage=parallel_gen_lineage"
                            ),
                            exc=exc,
                        )
        if not found_any:
            logger.info("No entries found for lineage extraction")

        logger.info(
            "Parallel lineage complete: entries_with_lineage=%s, processed=%s, "
            "no_lineage=%s, unsupported=%s, failed=%s, upstream_links=%s, edges=%s",
            entries_with_lineage,
            self.report.num_lineage_entries_processed,
            self.report.num_lineage_entries_without_lineage,
            self.report.num_lineage_entries_skipped_unsupported_type,
            self.report.num_lineage_entries_failed,
            self.report.num_lineage_upstream_links_found,
            self.report.num_lineage_edges_added,
        )
