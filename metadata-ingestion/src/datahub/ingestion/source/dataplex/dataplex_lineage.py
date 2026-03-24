"""Dataplex lineage extraction module."""

from __future__ import annotations

import collections
import logging
from dataclasses import dataclass
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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_ids import (
    extract_datahub_dataset_name_from_fqn,
    is_supported_lineage_entry_type,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

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
        entry_id: The upstream entry ID in Dataplex format
        audit_stamp: When this lineage was observed
        lineage_type: Type of lineage (TRANSFORMED, COPY, etc.)
    """

    entry_id: str
    audit_stamp: datetime
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED


class DataplexLineageExtractor:
    """
    Extracts lineage information from Google Dataplex using the Data Lineage API.

    This class queries the Dataplex Lineage API to discover upstream and downstream
    relationships between entries and generates DataHub lineage metadata.
    """

    def __init__(
        self,
        config: DataplexConfig,
        report: DataplexReport,
        lineage_client: Optional[LineageClient] = None,
        redundant_run_skip_handler: Optional[Any] = None,
    ):
        """
        Initialize the lineage extractor.

        Args:
            config: Dataplex source configuration
            report: Source report for tracking metrics
            lineage_client: Optional pre-configured LineageClient
            redundant_run_skip_handler: Optional handler to skip redundant lineage runs
        """
        self.config = config
        self.report = report
        self.lineage_client = lineage_client
        self.redundant_run_skip_handler = redundant_run_skip_handler

        # Map dataset IDs to their upstream dependencies to enable efficient lineage lookups
        # during workunit generation without re-querying the Lineage API
        self.lineage_by_full_dataset_id: Dict[str, Set[LineageEdge]] = (
            collections.defaultdict(set)
        )

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
            # We only need multi-region name like US, EU, etc.
            parent = (
                f"projects/{project_id}/locations/{self.config.entries_locations[0]}"
            )

            # Get upstream lineage (where this entry is the target) - retries are handled inside _search_links_by_target
            upstream_links = self._search_links_by_target(parent, fully_qualified_name)
            for link in upstream_links:
                if link.source and link.source.fully_qualified_name:
                    lineage_data["upstream"].append(link.source.fully_qualified_name)

            # Get downstream lineage (where this entry is the source) - retries are handled inside _search_links_by_source
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
                self.report.num_lineage_entries_scanned += 1

            return lineage_data

        except Exception as e:
            # After retries are exhausted, report structured warning and continue
            self.report.num_lineage_entries_failed += 1
            self.report.report_warning(
                "Failed to get lineage for entry after retries. Continuing with other entries.",
                context=entry.dataplex_entry_short_name,
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
        entry_count = 0

        for entry in entry_data:
            entry_count += 1
            logger.debug(
                f"Processing entry {entry_count}: {entry.datahub_dataset_name} (platform: {entry.datahub_platform})"
            )
            lineage_data = self.get_lineage_for_entry(project_id, entry)

            if not lineage_data:
                continue

            if not is_supported_lineage_entry_type(
                entry.dataplex_entry_type_short_name
            ):
                continue

            # Convert upstream FQNs to LineageEdge objects
            for upstream_fqn in lineage_data.get("upstream", []):
                # Reuse this entry type's configured FQN parser to normalize lineage IDs.
                upstream_dataset_id = extract_datahub_dataset_name_from_fqn(
                    entry_type_or_short_name=entry.dataplex_entry_type_short_name,
                    fully_qualified_name=upstream_fqn,
                )

                if upstream_dataset_id:
                    edge = LineageEdge(
                        entry_id=upstream_dataset_id,
                        audit_stamp=datetime.now(timezone.utc),
                        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    # Use full dataset_id as key to avoid collisions between tables with same name
                    lineage_by_full_dataset_id[entry.datahub_dataset_name].add(edge)
                    logger.debug(
                        f"  Added lineage edge: {entry.datahub_dataset_name} <- {upstream_dataset_id}"
                    )

        self.lineage_by_full_dataset_id = lineage_by_full_dataset_id

        # Summary logging
        total_edges = sum(len(edges) for edges in lineage_by_full_dataset_id.values())
        entries_with_lineage = len(lineage_by_full_dataset_id)
        logger.info(
            f"Lineage map complete: {entries_with_lineage} entries with lineage, {total_edges} total edges"
        )

        return lineage_by_full_dataset_id

    def get_lineage_for_table(
        self, dataset_id: str, dataset_urn: str, platform: str
    ) -> Optional[UpstreamLineageClass]:
        """
        Build UpstreamLineageClass for a specific entry.

        Args:
            dataset_id: Full dataset ID (e.g., project.dataset.table for BigQuery)
            dataset_urn: DataHub URN for the dataset
            platform: Source platform for the entry (bigquery, gcs, etc.)

        Returns:
            UpstreamLineageClass object or None if no lineage exists
        """
        if dataset_id not in self.lineage_by_full_dataset_id:
            return None

        upstream_list: list[UpstreamClass] = []

        for lineage_edge in self.lineage_by_full_dataset_id[dataset_id]:
            # Generate URN for the upstream entry using the full dataset_id
            upstream_urn = builder.make_dataset_urn_with_platform_instance(
                platform=platform,
                name=lineage_edge.entry_id,
                platform_instance=None,
                env=self.config.env,
            )

            # Create table-level lineage
            upstream_class = UpstreamClass(
                dataset=upstream_urn,
                type=lineage_edge.lineage_type,
                auditStamp=AuditStampClass(
                    actor="urn:li:corpuser:datahub",
                    time=int(lineage_edge.audit_stamp.timestamp() * 1000),
                ),
            )
            upstream_list.append(upstream_class)
            # Report the lineage relationship
            self.report.report_lineage_relationship_created()

        if not upstream_list:
            return None

        return UpstreamLineageClass(upstreams=upstream_list)

    def gen_lineage(
        self,
        dataset_id: str,
        dataset_urn: str,
        platform: str,
        upstream_lineage: Optional[UpstreamLineageClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate lineage workunits for a dataset.

        Args:
            dataset_id: Full dataset ID (e.g., project.dataset.table for BigQuery)
            dataset_urn: DataHub URN for the dataset
            platform: Source platform (bigquery, gcs, etc.)
            upstream_lineage: Optional pre-built UpstreamLineageClass

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if upstream_lineage is None:
            upstream_lineage = self.get_lineage_for_table(
                dataset_id, dataset_urn, platform
            )

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
                        entry.datahub_platform,
                    )
                except Exception as e:
                    self.report.num_lineage_entries_failed += 1
                    self.report.report_warning(
                        "Failed to generate lineage for entry.",
                        context=entry.datahub_dataset_name,
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
                            entry.datahub_platform,
                        )
                    except Exception as e:
                        self.report.num_lineage_entries_failed += 1
                        self.report.report_warning(
                            "Failed to generate lineage for entry.",
                            context=entry.datahub_dataset_name,
                            exc=e,
                        )

                # Clear lineage map after processing batch to free memory
                self.lineage_by_full_dataset_id.clear()
                logger.debug(f"Cleared lineage map after batch {batch_idx + 1}")

            logger.info(f"Completed lineage extraction for all {num_batches} batches")
