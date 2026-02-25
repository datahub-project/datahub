"""Dataplex lineage extraction module."""

from __future__ import annotations

import collections
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Set

from google.api_core import exceptions as google_exceptions
from google.cloud import dataplex_v1
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from google.cloud.datacatalog.lineage_v1 import LineageClient

# google-cloud-datacatalog-lineage is a required dependency (pinned to 0.2.2 in setup.py)
from google.cloud.datacatalog.lineage_v1 import EntityReference, SearchLinksRequest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntityDataTuple,
    make_entity_dataset_urn,
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
    Represents a lineage edge between two entities.

    This dataclass uses frozen=True, eq=True, and order=True because:
    - frozen=True: Makes instances immutable and hashable, allowing them to be stored in sets
    - eq=True: Enables equality comparison to detect and prevent duplicate edges in sets
    - order=True: Provides consistent ordering for deterministic iteration and sorting

    LineageEdge instances are stored in Set[LineageEdge] (see lineage_map), requiring
    immutability and hashability.

    Attributes:
        entity_id: The upstream entity ID in Dataplex format
        audit_stamp: When this lineage was observed
        lineage_type: Type of lineage (TRANSFORMED, COPY, etc.)
    """

    entity_id: str
    audit_stamp: datetime
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED


class DataplexLineageExtractor:
    """
    Extracts lineage information from Google Dataplex using the Data Lineage API.

    This class queries the Dataplex Lineage API to discover upstream and downstream
    relationships between entities and generates DataHub lineage metadata.
    """

    def __init__(
        self,
        config: DataplexConfig,
        report: DataplexReport,
        lineage_client: Optional[LineageClient] = None,
        dataplex_client: Optional[dataplex_v1.DataplexServiceClient] = None,
        redundant_run_skip_handler: Optional[Any] = None,
    ):
        """
        Initialize the lineage extractor.

        Args:
            config: Dataplex source configuration
            report: Source report for tracking metrics
            lineage_client: Optional pre-configured LineageClient
            dataplex_client: Optional pre-configured DataplexServiceClient
            redundant_run_skip_handler: Optional handler to skip redundant lineage runs
        """
        self.config = config
        self.report = report
        self.lineage_client = lineage_client
        self.dataplex_client = dataplex_client
        self.redundant_run_skip_handler = redundant_run_skip_handler

        # Map entity IDs to their upstream dependencies to enable efficient lineage lookups
        # during workunit generation without re-querying the Lineage API
        self.lineage_map: Dict[str, Set[LineageEdge]] = collections.defaultdict(set)

    def get_lineage_for_entity(
        self, project_id: str, entity: EntityDataTuple
    ) -> Optional[Dict[str, list]]:
        """
        Get lineage information for a specific Dataplex entity with automatic retries.

        This method uses tenacity to automatically retry transient errors (timeouts, rate limits, etc.)
        with exponential backoff. After retries are exhausted, logs a warning and continues.

        Args:
            project_id: GCP project ID
            entity: EntityDataTuple

        Returns:
            Dictionary with 'upstream' and 'downstream' lists of entity FQNs,
            or None if lineage extraction is disabled or fails after retries
        """
        if not self.config.include_lineage or not self.lineage_client:
            return None

        try:
            fully_qualified_name = self._construct_fqn(
                entity.source_platform,
                project_id,
                entity.dataset_id,
                entity.entity_id,
                entity.is_entry,
            )
            lineage_data: dict[str, list[Any]] = {"upstream": [], "downstream": []}
            # We only need multi-region name like US, EU, etc., specific region name like us-central1, eu-central1, etc. does not work
            parent = (
                f"projects/{project_id}/locations/{self.config.location.split('-')[0]}"
            )

            # Get upstream lineage (where this entity is the target) - retries are handled inside _search_links_by_target
            upstream_links = self._search_links_by_target(parent, fully_qualified_name)
            for link in upstream_links:
                if link.source and link.source.fully_qualified_name:
                    lineage_data["upstream"].append(link.source.fully_qualified_name)

            # Get downstream lineage (where this entity is the source) - retries are handled inside _search_links_by_source
            downstream_links = self._search_links_by_source(
                parent, fully_qualified_name
            )
            for link in downstream_links:
                if link.target and link.target.fully_qualified_name:
                    lineage_data["downstream"].append(link.target.fully_qualified_name)

            if lineage_data["upstream"] or lineage_data["downstream"]:
                logger.debug(
                    f"Found lineage for {entity.entity_id}: "
                    f"{len(lineage_data['upstream'])} upstream, "
                    f"{len(lineage_data['downstream'])} downstream"
                )
                self.report.num_lineage_entries_scanned += 1

            return lineage_data

        except Exception as e:
            # After retries are exhausted, report structured warning and continue
            self.report.num_lineage_entries_failed += 1
            self.report.report_warning(
                "Failed to get lineage for entity after retries. Continuing with other entities.",
                context=entity.entity_id,
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
        Implementation of searching for lineage links where the entity is a target (to find upstream).
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
        Search for lineage links where the entity is a target (to find upstream).

        Applies configurable retry logic with exponential backoff for transient errors.

        Args:
            parent: Parent resource path (projects/{project}/locations/{location})
            fully_qualified_name: FQN of the entity

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
        Implementation of search for lineage links where the entity is a source (to find downstream).

        Note: The Google Cloud Lineage API client automatically handles pagination.
        The search_links() method returns a SearchLinksPager that fetches all pages
        transparently when converted to a list, so no manual pagination is needed.

        Args:
            parent: Parent resource path (projects/{project}/locations/{location})
            fully_qualified_name: FQN of the entity

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
            fully_qualified_name: FQN of the entity

        Returns:
            List of Link objects (all pages automatically retrieved)
        """
        retry_decorator = self._get_retry_decorator()
        retrying_func = retry_decorator(self._search_links_by_source_impl)
        return retrying_func(parent, fully_qualified_name)

    def _construct_fqn(
        self,
        platform: str,
        project_id: str,
        dataset_id: str,
        entity_id: str,
        is_entry: bool = False,
    ) -> str:
        """
        Construct a fully qualified name for an entity based on platform.

        The FQN format varies by platform per Google Cloud Lineage API:
        - BigQuery: bigquery:{project}.{dataset}.{table}
        - GCS: gcs:{bucket} or gcs:{bucket}.{path/to/file}

        Args:
            platform: Source platform ("bigquery" or "gcs")
            project_id: GCP project ID
            dataset_id: Dataset ID format depends on is_entry:
                - For entries (is_entry=True): Full path like "project.dataset.table"
                - For entities (is_entry=False): Just dataset name like "dataset"
            entity_id: Entity ID (table name for BigQuery, file path for GCS)
            is_entry: True if from Entries API, False if from Entities API

        Returns:
            Fully qualified name string in the format expected by Google Lineage API
        """
        if platform == "bigquery":
            # BigQuery format: bigquery:{project}.{dataset}.{table}
            if is_entry:
                # For entries, dataset_id already contains "project.dataset.table"
                # so we just prepend the platform
                return f"bigquery:{dataset_id}"
            else:
                # For entities, construct from components
                return f"bigquery:{project_id}.{dataset_id}.{entity_id}"
        elif platform == "gcs":
            # GCS format: gcs:{bucket} or gcs:{bucket}.{path}
            if is_entry:
                # For entries, dataset_id already contains the full path
                return f"gcs:{dataset_id}"
            else:
                # For entities, construct from components
                # entity_id might be empty (bucket-level) or a path
                if entity_id and entity_id != dataset_id:
                    # If entity_id is a path, construct gcs:{bucket}.{path}
                    return f"gcs:{dataset_id}.{entity_id}"
                else:
                    # Bucket-level resource
                    return f"gcs:{dataset_id}"
        else:
            # Fallback for unknown platforms (shouldn't happen in practice)
            logger.warning(
                f"Unknown platform '{platform}' for FQN construction, using generic format"
            )
            if is_entry:
                return f"{platform}:{dataset_id}"
            else:
                return f"{platform}:{project_id}.{dataset_id}.{entity_id}"

    def build_lineage_map(
        self, project_id: str, entity_data: Iterable[EntityDataTuple]
    ) -> Dict[str, Set[LineageEdge]]:
        """
        Build a map of entity lineage for multiple entities.

        Args:
            project_id: GCP project ID
            entity_data: Iterable of EntityDataTuple objects to process

        Returns:
            Dictionary mapping entity IDs to sets of LineageEdge objects
        """
        logger.info(f"Starting lineage map build for project {project_id}")
        lineage_map: Dict[str, Set[LineageEdge]] = collections.defaultdict(set)
        entity_count = 0

        for entity in entity_data:
            entity_count += 1
            logger.debug(
                f"Processing entity {entity_count}: {entity.entity_id} (platform: {entity.source_platform})"
            )
            lineage_data = self.get_lineage_for_entity(project_id, entity)

            if not lineage_data:
                continue

            # Convert upstream FQNs to LineageEdge objects
            for upstream_fqn in lineage_data.get("upstream", []):
                # Extract entity ID from FQN
                upstream_entity_id = self._extract_entity_id_from_fqn(upstream_fqn)

                if upstream_entity_id:
                    edge = LineageEdge(
                        entity_id=upstream_entity_id,
                        audit_stamp=datetime.now(timezone.utc),
                        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    lineage_map[entity.entity_id].add(edge)
                    logger.debug(
                        f"  Added lineage edge: {entity.entity_id} <- {upstream_entity_id}"
                    )

        self.lineage_map = lineage_map

        # Summary logging
        total_edges = sum(len(edges) for edges in lineage_map.values())
        entities_with_lineage = len(lineage_map)
        logger.info(
            f"Lineage map complete: {entities_with_lineage} entities with lineage, {total_edges} total edges"
        )

        return lineage_map

    def _extract_entity_id_from_fqn(self, fqn: str) -> Optional[str]:
        """
        Extract entity ID from a fully qualified name.

        Handles platform-specific FQN formats:
        - BigQuery: bigquery:{project}.{dataset}.{table} -> {project}.{dataset}.{table}
        - GCS: gcs:{bucket}.{path} -> {bucket}.{path}
        - GCS: gcs:{bucket} -> {bucket}

        Args:
            fqn: Fully qualified name in format "{platform}:{identifier}"

        Returns:
            Entity ID (everything after the platform prefix) or None if extraction fails
        """
        try:
            if ":" in fqn:
                platform, entity_part = fqn.split(":", 1)

                # Validate that we have a known platform
                if platform not in ["bigquery", "gcs", "dataplex"]:
                    logger.warning(f"Unexpected platform '{platform}' in FQN: {fqn}")

                return entity_part
            else:
                # No platform prefix, return as-is (shouldn't happen in practice)
                logger.warning(f"FQN missing platform prefix: {fqn}")
                return fqn
        except Exception as e:
            logger.error(f"Failed to extract entity ID from FQN '{fqn}': {e}")
            return None

    def get_lineage_for_table(
        self, entity_id: str, dataset_urn: str, platform: str
    ) -> Optional[UpstreamLineageClass]:
        """
        Build UpstreamLineageClass for a specific entity.

        Args:
            entity_id: Entity ID
            dataset_urn: DataHub URN for the dataset
            platform: Source platform for the entity (bigquery, gcs, etc.)

        Returns:
            UpstreamLineageClass object or None if no lineage exists
        """
        if entity_id not in self.lineage_map:
            return None

        upstream_list: list[UpstreamClass] = []

        for lineage_edge in self.lineage_map[entity_id]:
            # Generate URN for the upstream entity
            # Extract project_id from entity_id (format: project_id.dataset_id.entity_name)
            if "." in lineage_edge.entity_id:
                parts = lineage_edge.entity_id.split(".", 2)
                if len(parts) == 3:
                    project_id, dataset_id, entity_name = parts
                elif len(parts) == 2:
                    # GCS format might be bucket.path or just bucket
                    dataset_id, entity_name = parts
                    project_id = (
                        self.config.project_ids[0]
                        if self.config.project_ids
                        else "unknown"
                    )
                else:
                    # Fallback if format is unexpected
                    project_id = (
                        self.config.project_ids[0]
                        if self.config.project_ids
                        else "unknown"
                    )
                    dataset_id = "unknown"
                    entity_name = lineage_edge.entity_id
            else:
                # Fallback if format is different
                project_id = (
                    self.config.project_ids[0] if self.config.project_ids else "unknown"
                )
                dataset_id = "unknown"
                entity_name = lineage_edge.entity_id

            # Use the same platform as the downstream entity
            # This assumes lineage is typically within the same platform
            upstream_urn = make_entity_dataset_urn(
                project_id=project_id,
                entity_id=entity_name,
                platform=platform,
                env=self.config.env,
                dataset_id=dataset_id,
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
        entity_id: str,
        dataset_urn: str,
        platform: str,
        upstream_lineage: Optional[UpstreamLineageClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate lineage workunits for a dataset.

        Args:
            entity_id: Entity ID
            dataset_urn: DataHub URN for the dataset
            platform: Source platform (bigquery, gcs, etc.)
            upstream_lineage: Optional pre-built UpstreamLineageClass

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if upstream_lineage is None:
            upstream_lineage = self.get_lineage_for_table(
                entity_id, dataset_urn, platform
            )

        if upstream_lineage is None:
            return

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=upstream_lineage
        ).as_workunit()

    def get_lineage_workunits(
        self, project_id: str, entity_data: Iterable[EntityDataTuple]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Main entry point to get lineage workunits for multiple entities.

        Processes entities in batches to reduce memory consumption for large deployments.
        Batch size is controlled by config.batch_size (default: 1000).
        Set to -1 to disable batching and process all entities at once.

        Args:
            project_id: GCP project ID
            entity_data: Iterable of EntityDataTuple objects

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if not self.config.include_lineage:
            logger.info("Lineage extraction is disabled")
            return

        logger.info(f"Extracting lineage for project {project_id}")

        # Convert to list to allow multiple iterations and get total count
        entity_list = list(entity_data)
        total_entities = len(entity_list)

        # Check if batching is disabled (None) or batch size >= total entities
        if self.config.batch_size is None or self.config.batch_size >= total_entities:
            logger.info(f"Processing all {total_entities} entities in a single batch")
            # Process all entities at once (original behavior)
            self.build_lineage_map(project_id, entity_list)

            for entity in entity_list:
                # Construct dataset URN based on whether this is from Entries API or Entities API
                if entity.is_entry:
                    import datahub.emitter.mce_builder as builder

                    dataset_urn = builder.make_dataset_urn_with_platform_instance(
                        platform=entity.source_platform,
                        name=entity.dataset_id,
                        platform_instance=None,
                        env=self.config.env,
                    )
                else:
                    # For entities, use hierarchical naming (project.dataset.entity)
                    dataset_urn = make_entity_dataset_urn(
                        project_id=project_id,
                        entity_id=entity.entity_id,
                        platform=entity.source_platform,
                        env=self.config.env,
                        dataset_id=entity.dataset_id,
                    )

                try:
                    yield from self.gen_lineage(
                        entity.entity_id, dataset_urn, entity.source_platform
                    )
                except Exception as e:
                    self.report.num_lineage_entries_failed += 1
                    self.report.report_warning(
                        "Failed to generate lineage for entity.",
                        context=entity.entity_id,
                        exc=e,
                    )
        else:
            # Process entities in batches
            batch_size = self.config.batch_size
            num_batches = (
                total_entities + batch_size - 1
            ) // batch_size  # Ceiling division

            logger.info(
                f"Processing {total_entities} entities in {num_batches} batches "
                f"of {batch_size} (memory optimization enabled)"
            )

            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, total_entities)
                batch = entity_list[start_idx:end_idx]

                logger.info(
                    f"Processing batch {batch_idx + 1}/{num_batches} "
                    f"({len(batch)} entities: {start_idx} to {end_idx - 1})"
                )

                # Build lineage map for this batch only
                self.build_lineage_map(project_id, batch)

                # Generate workunits for entities in this batch
                for entity in batch:
                    # Construct dataset URN based on whether this is from Entries API or Entities API
                    if entity.is_entry:
                        import datahub.emitter.mce_builder as builder

                        dataset_urn = builder.make_dataset_urn_with_platform_instance(
                            platform=entity.source_platform,
                            name=entity.dataset_id,
                            platform_instance=None,
                            env=self.config.env,
                        )
                    else:
                        # For entities, use hierarchical naming (project.dataset.entity)
                        dataset_urn = make_entity_dataset_urn(
                            project_id=project_id,
                            entity_id=entity.entity_id,
                            platform=entity.source_platform,
                            env=self.config.env,
                            dataset_id=entity.dataset_id,
                        )

                    try:
                        yield from self.gen_lineage(
                            entity.entity_id, dataset_urn, entity.source_platform
                        )
                    except Exception as e:
                        self.report.num_lineage_entries_failed += 1
                        self.report.report_warning(
                            "Failed to generate lineage for entity.",
                            context=entity.entity_id,
                            exc=e,
                        )

                # Clear lineage map after processing batch to free memory
                self.lineage_map.clear()
                logger.debug(f"Cleared lineage map after batch {batch_idx + 1}")

            logger.info(f"Completed lineage extraction for all {num_batches} batches")
