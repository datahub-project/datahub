"""Dataplex lineage extraction module."""

from __future__ import annotations

import collections
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Set

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

from google.cloud.datacatalog_lineage import EntityReference, SearchLinksRequest

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_ids import (
    build_dataset_urn_from_fqn_only,
    is_supported_lineage_entry_type,
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


@dataclass(order=True, eq=True, frozen=True)
class LineageEdge:
    """
    Represents a lineage edge between two entries.

    This dataclass uses frozen=True, eq=True, and order=True because:
    - frozen=True: Makes instances immutable and hashable, allowing them to be stored in sets
    - eq=True: Enables equality comparison to detect and prevent duplicate edges in sets
    - order=True: Provides consistent ordering for deterministic iteration and sorting

    LineageEdge instances are stored in Set[LineageEdge] (see lineage_by_full_dataset_id), requiring
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
    lineage_api_timer: PerfTimer = field(default_factory=PerfTimer)

    def report_lineage_relationship_created(self, relationship: str) -> None:
        self.num_lineage_relationships_created += 1
        self.lineage_relationships_created_samples.append(relationship)
        logger.debug(f"Lineage relationship created: {relationship}")

    def report_lineage_entry_processed(self, entry_name: str) -> None:
        self.num_lineage_entries_processed += 1
        self.lineage_entries_processed_samples.append(entry_name)
        logger.debug(f"Lineage entry processed: {entry_name}")

    def report_lineage_entry_scanned(self, entry_name: str) -> None:
        self.num_lineage_entries_scanned += 1
        self.lineage_entries_scanned_samples.append(entry_name)
        logger.debug(f"Lineage entry has links: {entry_name}")

    def report_lineage_entry_without_lineage(
        self, entry_name: str, reason: str
    ) -> None:
        self.num_lineage_entries_without_lineage += 1
        self.lineage_entries_without_lineage_samples.append(
            f"entry={entry_name}, reason={reason}"
        )
        logger.debug(f"Lineage missing for entry {entry_name} (reason={reason})")

    def report_lineage_entry_skipped_unsupported_type(
        self, entry_name: str, entry_type: str
    ) -> None:
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
        self.num_lineage_downstream_links_found += count
        self.lineage_downstream_links_found_samples.append(
            f"entry={entry_name}, count={count}"
        )
        logger.debug(f"Lineage downstream links observed for {entry_name}: {count}")

    def report_lineage_edge_added(
        self, downstream_dataset_id: str, upstream_dataset_urn: str
    ) -> None:
        self.num_lineage_edges_added += 1
        self.lineage_edges_added_samples.append(
            f"{downstream_dataset_id}<-{upstream_dataset_urn}"
        )
        logger.debug(
            f"Lineage edge added: {downstream_dataset_id} <- {upstream_dataset_urn}"
        )

    def report_lineage_entry_failed(self, entry_name: str, stage: str) -> None:
        self.num_lineage_entries_failed += 1
        self.lineage_entries_failed_samples.append(f"entry={entry_name}, stage={stage}")
        logger.debug(f"Lineage entry failed: {entry_name} (stage={stage})")


class DataplexLineageExtractor:
    """
    Extracts lineage information from Google Dataplex using the Data Lineage API.

    This class queries the Dataplex Lineage API to discover upstream and downstream
    relationships between entries and generates DataHub lineage metadata.
    """

    def __init__(
        self,
        config: DataplexConfig,
        report: DataplexLineageReport,
        source_report: SourceReport,
        lineage_client: Optional[LineageClient] = None,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
    ):
        """
        Initialize the lineage extractor.

        Args:
            config: Dataplex source configuration
            report: Lineage report for lineage-specific counters
            source_report: Source report for warning/failure emission
            lineage_client: Optional pre-configured LineageClient
            redundant_run_skip_handler: Optional redundant lineage run skip handler
        """
        self.config = config
        self.report = report
        self.source_report = source_report
        self.lineage_client = lineage_client
        # TODO: Use redundant_run_skip_handler to short-circuit lineage calls when stateful
        # lineage ingestion determines this run is redundant.
        self.redundant_run_skip_handler = redundant_run_skip_handler

        # Map dataset IDs to their upstream dependencies to enable efficient lineage lookups
        # during workunit generation without re-querying the Lineage API
        self.lineage_by_full_dataset_id: Dict[str, Set[LineageEdge]] = (
            collections.defaultdict(set)
        )

    def _resolve_lineage_parent(self, project_id: str, entry: EntryDataTuple) -> str:
        """Resolve Data Lineage API parent using the tracked entry location."""
        return f"projects/{project_id}/locations/{entry.dataplex_location}"

    def get_lineage_for_entry(
        self, project_id: str, entry: EntryDataTuple
    ) -> Optional[Dict[str, list]]:
        """
        Get lineage information for a specific Dataplex entry with automatic retries.

        This method uses tenacity to automatically retry transient errors (timeouts, rate limits, etc.)
        with exponential backoff. After retries are exhausted, logs a warning and continues.

        Args:
            project_id: GCP project ID
            entry: EntryDataTuple

        Returns:
            Dictionary with 'upstream' and 'downstream' lists of entry FQNs,
            or None if lineage extraction is disabled or fails after retries
        """
        if not self.config.include_lineage or not self.lineage_client:
            return None

        try:
            fully_qualified_name = entry.dataplex_entry_fqn
            lineage_data: dict[str, list[Any]] = {"upstream": [], "downstream": []}
            parent = self._resolve_lineage_parent(project_id, entry)

            # Get upstream lineage (where this entry is the target) - retries are handled inside _search_links_by_target
            with self.report.lineage_api_timer:
                upstream_links = self._search_links_by_target(
                    parent, fully_qualified_name
                )
            for link in upstream_links:
                if link.source and link.source.fully_qualified_name:
                    lineage_data["upstream"].append(link.source.fully_qualified_name)

            # Get downstream lineage (where this entry is the source) - retries are handled inside _search_links_by_source
            with self.report.lineage_api_timer:
                downstream_links = self._search_links_by_source(
                    parent, fully_qualified_name
                )
            for link in downstream_links:
                if link.target and link.target.fully_qualified_name:
                    lineage_data["downstream"].append(link.target.fully_qualified_name)

            if lineage_data["upstream"] or lineage_data["downstream"]:
                logger.debug(
                    f"Found lineage for {entry.dataplex_entry_short_name}: "
                    f"{len(lineage_data['upstream'])} upstream, "
                    f"{len(lineage_data['downstream'])} downstream"
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
                    f"project_id={project_id}, "
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
                multiplier=self.config.lineage_retry_backoff_multiplier, min=2, max=10
            ),
            stop=stop_after_attempt(self.config.lineage_max_retries),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    def _search_links_by_target_impl(
        self, parent: str, fully_qualified_name: str
    ) -> Iterable:
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
        request = SearchLinksRequest(parent=parent, target=target)
        # Convert pager to list - this automatically handles pagination
        results = list(self.lineage_client.search_links(request=request))
        logger.debug(
            f"Found {len(results)} upstream lineage link(s) for {fully_qualified_name}"
        )
        return results

    def _search_links_by_target(
        self, parent: str, fully_qualified_name: str
    ) -> Iterable:
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

    def _search_links_by_source_impl(
        self, parent: str, fully_qualified_name: str
    ) -> Iterable:
        """
        Implementation of search for lineage links where the entry is a source (to find downstream).

        Note: The Google Cloud Lineage API client automatically handles pagination.
        The search_links() method returns a SearchLinksPager that fetches all pages
        transparently when converted to a list, so no manual pagination is needed.

        Args:
            parent: Parent resource path (projects/{project}/locations/{location})
            fully_qualified_name: FQN of the entry

        Returns:
            List of Link objects (all pages automatically retrieved)

        Raises:
            RuntimeError: If lineage client is not initialized
            Exception: If the lineage API call fails
        """
        if self.lineage_client is None:
            raise RuntimeError("Lineage client is not initialized")
        logger.debug(f"Searching downstream lineage for FQN: {fully_qualified_name}")
        source = EntityReference(fully_qualified_name=fully_qualified_name)
        request = SearchLinksRequest(parent=parent, source=source)
        # Convert pager to list - this automatically handles pagination
        results = list(self.lineage_client.search_links(request=request))
        logger.debug(
            f"Found {len(results)} downstream lineage link(s) for {fully_qualified_name}"
        )
        return results

    def _search_links_by_source(
        self, parent: str, fully_qualified_name: str
    ) -> Iterable:
        """
        Wrapper that applies configurable retry logic to downstream lineage search.

        Args:
            parent: Parent resource path (projects/{project}/locations/{location})
            fully_qualified_name: FQN of the entry

        Returns:
            List of Link objects (all pages automatically retrieved)
        """
        retry_decorator = self._get_retry_decorator()
        retrying_func = retry_decorator(self._search_links_by_source_impl)
        return retrying_func(parent, fully_qualified_name)

    def build_lineage_map(
        self, project_id: str, entry_data: Iterable[EntryDataTuple]
    ) -> Dict[str, Set[LineageEdge]]:
        """
        Build a map of entry lineage for multiple entries.

        Args:
            project_id: GCP project ID
            entry_data: Iterable of EntryDataTuple objects to process

        Returns:
            Dictionary mapping dataset IDs (full paths) to sets of LineageEdge objects
        """
        logger.info(f"Starting lineage map build for project {project_id}")
        lineage_by_full_dataset_id: Dict[str, Set[LineageEdge]] = (
            collections.defaultdict(set)
        )
        for entry_index, entry in enumerate(entry_data, start=1):
            self.report.report_lineage_entry_processed(entry.dataplex_entry_name)
            lineage_data = self.get_lineage_for_entry(project_id, entry)

            if not lineage_data:
                self.report.report_lineage_entry_without_lineage(
                    entry_name=entry.dataplex_entry_name,
                    reason="lineage_lookup_failed_or_unavailable",
                )
                continue
            upstream_count = len(lineage_data.get("upstream", []))
            downstream_count = len(lineage_data.get("downstream", []))
            self.report.report_lineage_upstream_links_found(
                entry_name=entry.dataplex_entry_name,
                count=upstream_count,
            )
            self.report.report_lineage_downstream_links_found(
                entry_name=entry.dataplex_entry_name,
                count=downstream_count,
            )
            if upstream_count == 0 and downstream_count == 0:
                self.report.report_lineage_entry_without_lineage(
                    entry_name=entry.dataplex_entry_name,
                    reason="empty_upstream_and_downstream",
                )
                continue

            if not is_supported_lineage_entry_type(
                entry.dataplex_entry_type_short_name
            ):
                self.report.report_lineage_entry_skipped_unsupported_type(
                    entry_name=entry.dataplex_entry_name,
                    entry_type=entry.dataplex_entry_type_short_name,
                )
                continue

            # Convert upstream FQNs to LineageEdge objects
            for upstream_fqn in lineage_data.get("upstream", []):
                # Upstream FQN may be cross-platform (e.g. pubsub->bigquery), so
                # normalize to DataHub URN using a mapping lookup driven only by FQN shape.
                upstream_dataset_urn = build_dataset_urn_from_fqn_only(
                    fully_qualified_name=upstream_fqn,
                    env=self.config.env,
                )

                if upstream_dataset_urn:
                    edge = LineageEdge(
                        upstream_datahub_urn=upstream_dataset_urn,
                        audit_stamp=datetime.now(timezone.utc),
                        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    # Use full dataset_id as key to avoid collisions between tables with same name
                    lineage_by_full_dataset_id[entry.datahub_dataset_name].add(edge)
                    self.report.report_lineage_edge_added(
                        downstream_dataset_id=entry.datahub_dataset_name,
                        upstream_dataset_urn=upstream_dataset_urn,
                    )
                    logger.debug(
                        "  Added lineage edge: %s <- %s",
                        entry.datahub_dataset_name,
                        upstream_dataset_urn,
                    )
                else:
                    self.report.report_lineage_upstream_fqn_skipped(
                        entry_name=entry.dataplex_entry_name,
                        upstream_fqn=upstream_fqn,
                    )
                    self.source_report.warning(
                        "Unable to normalize upstream Dataplex lineage FQN. Skipping upstream edge.",
                        title="Dataplex upstream lineage parse failed",
                        context=(
                            f"project_id={project_id}, "
                            f"dataplex_entry_name={entry.dataplex_entry_name}, "
                            f"datahub_dataset_name={entry.datahub_dataset_name}, "
                            f"entry_type={entry.dataplex_entry_type_short_name}, "
                            f"upstream_fqn={upstream_fqn}"
                        ),
                    )
            if entry_index % 1000 == 0:
                logger.info(
                    "Lineage map progress for project %s: processed=%s, scanned=%s, "
                    "no_lineage=%s, unsupported=%s, failed=%s, edges_added=%s, "
                    "upstream_parse_skipped=%s",
                    project_id,
                    self.report.num_lineage_entries_processed,
                    self.report.num_lineage_entries_scanned,
                    self.report.num_lineage_entries_without_lineage,
                    self.report.num_lineage_entries_skipped_unsupported_type,
                    self.report.num_lineage_entries_failed,
                    self.report.num_lineage_edges_added,
                    self.report.num_lineage_upstream_fqns_skipped,
                )

        self.lineage_by_full_dataset_id = lineage_by_full_dataset_id

        # Summary logging
        total_edges = sum(len(edges) for edges in lineage_by_full_dataset_id.values())
        entries_with_lineage = len(lineage_by_full_dataset_id)
        logger.info(
            "Lineage map complete for project %s: entries_with_lineage=%s, total_edges=%s, "
            "processed=%s, no_lineage=%s, unsupported=%s, failed=%s, upstream_parse_skipped=%s, "
            "upstream_links_observed=%s, downstream_links_observed=%s, edges_added=%s",
            project_id,
            entries_with_lineage,
            total_edges,
            self.report.num_lineage_entries_processed,
            self.report.num_lineage_entries_without_lineage,
            self.report.num_lineage_entries_skipped_unsupported_type,
            self.report.num_lineage_entries_failed,
            self.report.num_lineage_upstream_fqns_skipped,
            self.report.num_lineage_upstream_links_found,
            self.report.num_lineage_downstream_links_found,
            self.report.num_lineage_edges_added,
        )

        return lineage_by_full_dataset_id

    def get_lineage_for_table(
        self, dataset_id: str, dataset_urn: str
    ) -> Optional[UpstreamLineageClass]:
        """
        Build UpstreamLineageClass for a specific entry.

        Args:
            dataset_id: Full dataset ID (e.g., project.dataset.table for BigQuery)
            dataset_urn: DataHub URN for the dataset
        Returns:
            UpstreamLineageClass object or None if no lineage exists
        """
        if dataset_id not in self.lineage_by_full_dataset_id:
            return None

        upstream_list: list[UpstreamClass] = []

        for lineage_edge in self.lineage_by_full_dataset_id[dataset_id]:
            # Create table-level lineage
            upstream_class = UpstreamClass(
                dataset=lineage_edge.upstream_datahub_urn,
                type=lineage_edge.lineage_type,
                auditStamp=AuditStampClass(
                    actor="urn:li:corpuser:datahub",
                    time=int(lineage_edge.audit_stamp.timestamp() * 1000),
                ),
            )
            upstream_list.append(upstream_class)
            # Report the lineage relationship
            self.report.report_lineage_relationship_created(
                f"{dataset_id}<-{lineage_edge.upstream_datahub_urn}"
            )

        if not upstream_list:
            return None

        return UpstreamLineageClass(upstreams=upstream_list)

    def gen_lineage(
        self,
        dataset_id: str,
        dataset_urn: str,
        upstream_lineage: Optional[UpstreamLineageClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate lineage workunits for a dataset.

        Args:
            dataset_id: Full dataset ID (e.g., project.dataset.table for BigQuery)
            dataset_urn: DataHub URN for the dataset
            upstream_lineage: Optional pre-built UpstreamLineageClass

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if upstream_lineage is None:
            upstream_lineage = self.get_lineage_for_table(dataset_id, dataset_urn)

        if upstream_lineage is None:
            return

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=upstream_lineage
        ).as_workunit()

    def get_lineage_workunits(
        self, project_id: str, entry_data: Iterable[EntryDataTuple]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Main entry point to get lineage workunits for multiple entries.

        Processes entries in batches to reduce memory consumption for large deployments.
        Batch size is controlled by config.batch_size (default: 1000).
        Set to None to disable batching and process all entries at once.

        Args:
            project_id: GCP project ID
            entry_data: Iterable of EntryDataTuple objects

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if not self.config.include_lineage:
            logger.info("Lineage extraction is disabled")
            return

        logger.info(f"Extracting lineage for project {project_id}")

        # Convert to list to allow multiple iterations and get total count
        entry_list = list(entry_data)
        total_entries = len(entry_list)

        # Check if batching is disabled (None) or batch size >= total entries
        if self.config.batch_size is None or self.config.batch_size >= total_entries:
            logger.info(f"Processing all {total_entries} entries in a single batch")
            # Process all entries at once (original behavior)
            self.build_lineage_map(project_id, entry_list)

            for entry in entry_list:
                # Construct dataset URN - entries use full dataset_id path
                dataset_urn = builder.make_dataset_urn_with_platform_instance(
                    platform=entry.datahub_platform,
                    name=entry.datahub_dataset_name,
                    platform_instance=None,
                    env=self.config.env,
                )

                try:
                    yield from self.gen_lineage(
                        entry.datahub_dataset_name,
                        dataset_urn,
                    )
                except Exception as e:
                    self.report.report_lineage_entry_failed(
                        entry_name=entry.dataplex_entry_name,
                        stage="gen_lineage_single_batch",
                    )
                    self.source_report.warning(
                        "Failed to generate lineage for entry.",
                        context=(
                            f"project_id={project_id}, "
                            f"dataplex_entry_name={entry.dataplex_entry_name}, "
                            f"datahub_dataset_name={entry.datahub_dataset_name}, "
                            "stage=gen_lineage_single_batch"
                        ),
                        exc=e,
                    )
        else:
            # Process entries in batches
            batch_size = self.config.batch_size
            num_batches = (
                total_entries + batch_size - 1
            ) // batch_size  # Ceiling division

            logger.info(
                f"Processing {total_entries} entries in {num_batches} batches "
                f"of {batch_size} (memory optimization enabled)"
            )

            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, total_entries)
                batch = entry_list[start_idx:end_idx]

                logger.info(
                    f"Processing batch {batch_idx + 1}/{num_batches} "
                    f"({len(batch)} entries: {start_idx} to {end_idx - 1})"
                )

                # Build lineage map for this batch only
                self.build_lineage_map(project_id, batch)

                # Generate workunits for entries in this batch
                for entry in batch:
                    # Construct dataset URN - entries use full dataset_id path
                    dataset_urn = builder.make_dataset_urn_with_platform_instance(
                        platform=entry.datahub_platform,
                        name=entry.datahub_dataset_name,
                        platform_instance=None,
                        env=self.config.env,
                    )

                    try:
                        yield from self.gen_lineage(
                            entry.datahub_dataset_name,
                            dataset_urn,
                        )
                    except Exception as e:
                        self.report.report_lineage_entry_failed(
                            entry_name=entry.dataplex_entry_name,
                            stage=f"gen_lineage_batch_{batch_idx + 1}",
                        )
                        self.source_report.warning(
                            "Failed to generate lineage for entry.",
                            context=(
                                f"project_id={project_id}, "
                                f"dataplex_entry_name={entry.dataplex_entry_name}, "
                                f"datahub_dataset_name={entry.datahub_dataset_name}, "
                                f"stage=gen_lineage_batch_{batch_idx + 1}"
                            ),
                            exc=e,
                        )

                # Clear lineage map after processing batch to free memory
                self.lineage_by_full_dataset_id.clear()
                logger.debug(f"Cleared lineage map after batch {batch_idx + 1}")

            logger.info(f"Completed lineage extraction for all {num_batches} batches")
