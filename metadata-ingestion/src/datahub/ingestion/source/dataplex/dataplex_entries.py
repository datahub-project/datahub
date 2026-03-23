"""Entry processing utilities for Dataplex source (Universal Catalog/Entries API)."""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING, Iterable, Optional

from google.cloud import dataplex_v1

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_containers import (
    track_bigquery_container,
)
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntryDataTuple,
    make_audit_stamp,
    parse_entry_fqn,
)
from datahub.ingestion.source.dataplex.dataplex_properties import (
    extract_entry_custom_properties,
)
from datahub.ingestion.source.dataplex.dataplex_schema import (
    extract_schema_from_entry_aspects,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    TimeStampClass,
)
from datahub.metadata.urns import DataPlatformUrn

if TYPE_CHECKING:
    from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)


@dataclass
class DataplexEntriesReport:
    """Phase 2 design for entry-processing observability metrics.

    Tracks high-level counters and lossy samples for filtered and processed
    entry groups / entries. This report is intentionally scoped to the entries
    processing loop and can later be folded into DataplexReport.
    """

    sample_limit: int = 20

    entry_groups_seen: int = 0
    entry_groups_filtered: int = 0
    entry_group_filtered_samples: list[str] = field(default_factory=list)

    entries_seen: int = 0
    entries_filtered_by_pattern: int = 0
    entry_pattern_filtered_samples: list[str] = field(default_factory=list)
    entries_filtered_by_fqn_pattern: int = 0
    entry_fqn_filtered_samples: list[str] = field(default_factory=list)

    entries_processed: int = 0
    entries_processed_samples: list[str] = field(default_factory=list)

    def _append_sample(self, samples: list[str], value: str) -> None:
        if len(samples) < self.sample_limit:
            samples.append(value)

    def report_entry_group_seen(self) -> None:
        self.entry_groups_seen += 1

    def report_entry_group_filtered(self, entry_group_name: str) -> None:
        self.entry_groups_filtered += 1
        self._append_sample(self.entry_group_filtered_samples, entry_group_name)

    def report_entry_seen(self) -> None:
        self.entries_seen += 1

    def report_entry_filtered_by_pattern(self, entry_name: str) -> None:
        self.entries_filtered_by_pattern += 1
        self._append_sample(self.entry_pattern_filtered_samples, entry_name)

    def report_entry_filtered_by_fqn_pattern(self, entry_fqn: str) -> None:
        self.entries_filtered_by_fqn_pattern += 1
        self._append_sample(self.entry_fqn_filtered_samples, entry_fqn)

    def report_entry_processed(self, entry_name: str) -> None:
        self.entries_processed += 1
        self._append_sample(self.entries_processed_samples, entry_name)


class DataplexEntriesProcessor:
    """Phase 2 design surface for class-based Dataplex entry processing.

    This class is intentionally signatures-only in Phase 2. Phase 3 will move
    the full processing loop here and switch DataplexSource to use it.
    """

    def __init__(
        self,
        config: DataplexConfig,
        catalog_client: dataplex_v1.CatalogServiceClient,
        report: DataplexEntriesReport,
        entry_data_by_project: dict[str, set[EntryDataTuple]],
        entry_data_lock: Lock,
        bq_containers: dict[str, set[str]],
        bq_containers_lock: Lock,
        construct_mcps_fn: Callable[
            [str, list], Iterable[MetadataChangeProposalWrapper]
        ],
    ) -> None:
        self.config = config
        self.catalog_client = catalog_client
        self.report = report
        self.entry_data_by_project = entry_data_by_project
        self.entry_data_lock = entry_data_lock
        self.bq_containers = bq_containers
        self.bq_containers_lock = bq_containers_lock
        self.construct_mcps_fn = construct_mcps_fn

    def process_project(self, project_id: str) -> Iterable["Entity"]:
        """Iterate all configured locations for project-level entry processing."""
        raise NotImplementedError("Phase 2 signatures only")

    def process_location(self, project_id: str, location: str) -> Iterable["Entity"]:
        """Process all eligible entry groups and entries for one location."""
        raise NotImplementedError("Phase 2 signatures only")

    def list_entry_groups(
        self, project_id: str, location: str
    ) -> Iterable[dataplex_v1.EntryGroup]:
        """List entry groups for a ``(project_id, location)`` pair."""
        raise NotImplementedError("Phase 2 signatures only")

    def collect_entries(
        self,
        project_id: str,
        location: str,
        entry_group: dataplex_v1.EntryGroup,
    ) -> list[dataplex_v1.Entry]:
        """Collect and deduplicate entries from list_entries + Spanner workaround."""
        raise NotImplementedError("Phase 2 signatures only")

    def collect_spanner_entries(
        self, project_id: str, location: str
    ) -> list[dataplex_v1.Entry]:
        """Collect Spanner entries via search_entries workaround."""
        raise NotImplementedError("Phase 2 signatures only")

    def deduplicate_entries_by_name(
        self, entries: Iterable[dataplex_v1.Entry]
    ) -> list[dataplex_v1.Entry]:
        """Deduplicate entries by unique entry ``name``."""
        raise NotImplementedError("Phase 2 signatures only")

    def should_process_entry_group(self, entry_group_name: str) -> bool:
        """Evaluate ``filter_config.entry_groups.pattern``."""
        raise NotImplementedError("Phase 2 signatures only")

    def should_process_entry(self, entry: dataplex_v1.Entry) -> bool:
        """Apply entry-level ``pattern`` and ``fqn_pattern`` filters."""
        raise NotImplementedError("Phase 2 signatures only")

    def build_entity_for_entry(self, entry: dataplex_v1.Entry) -> Optional["Entity"]:
        """Map Dataplex entry to DataHub SDK v2 Dataset/Container entity."""
        raise NotImplementedError("Phase 2 signatures only")

    def build_entry_container_urn(self, entry: dataplex_v1.Entry) -> Optional[str]:
        """Build container-relationship URN for entry parent."""
        raise NotImplementedError("Phase 2 signatures only")


def process_entry(
    project_id: str,
    entry: dataplex_v1.Entry,
    entry_group_id: str,
    config: DataplexConfig,
    entry_data_by_project: dict[str, set[EntryDataTuple]],
    entry_data_lock: Lock,
    bq_containers: dict[str, set[str]],
    bq_containers_lock: Lock,
    construct_mcps_fn: Callable[[str, list], Iterable[MetadataChangeProposalWrapper]],
) -> Iterable[MetadataChangeProposalWrapper]:
    """Process a single entry from Universal Catalog.

    Args:
        project_id: GCP project ID
        entry: Entry object from Catalog API
        entry_group_id: Entry group ID
        config: Dataplex configuration object
        entry_data_by_project: Mapping of project IDs to entry data tuples
        entry_data_lock: Lock for entry_data_by_project access
        bq_containers: BigQuery containers cache
        bq_containers_lock: Lock for bq_containers access
        construct_mcps_fn: Function to construct MCPs from dataset URN and aspects

    Yields:
        MetadataChangeProposalWrapper objects for the entry
    """
    entry_id = entry.name.split("/")[-1]

    if not entry.fully_qualified_name:
        logger.debug(f"Entry {entry_id} has no fully_qualified_name, skipping")
        return

    fqn = entry.fully_qualified_name
    logger.debug(f"Processing entry with FQN: {fqn}")

    # Apply dataset pattern filter to entry_id
    if not config.filter_config.entries.dataset_pattern.allowed(entry_id):
        logger.debug(f"Entry {entry_id} filtered out by entries.dataset_pattern")
        return

    # Parse the FQN to determine platform and dataset_id
    source_platform, dataset_id = parse_entry_fqn(fqn)
    if not source_platform or not dataset_id:
        logger.warning(f"Could not parse FQN {fqn} for entry {entry_id}, skipping")
        return

    # Validate that FQN has a table/file component (not just zone/asset metadata)
    if ":" in fqn:
        _, resource_path = fqn.split(":", 1)

        # For BigQuery: should be project.dataset.table (3 parts minimum)
        if source_platform == "bigquery":
            parts = resource_path.split(".")
            if len(parts) < 3:
                logger.debug(
                    f"Skipping entry {entry_id} with FQN {fqn} - missing table name (only {len(parts)} parts)"
                )
                return
            # Check if the table name looks like a zone or asset (common pattern suffixes)
            table_name = parts[-1]
            if any(
                suffix in table_name.lower()
                for suffix in ["_zone", "_asset", "zone1", "asset1"]
            ):
                logger.debug(
                    f"Skipping entry {entry_id} with FQN {fqn} - table name '{table_name}' appears to be zone/asset metadata"
                )
                return

        # For GCS: should be bucket/path (2 parts minimum)
        elif source_platform == "gcs":
            parts = resource_path.split("/")
            if len(parts) < 2:
                logger.debug(
                    f"Skipping entry {entry_id} with FQN {fqn} - missing file path (only {len(parts)} parts)"
                )
                return
            # Check if the file/object name looks like an asset
            object_name = parts[-1]
            if any(suffix in object_name.lower() for suffix in ["_asset", "asset1"]):
                logger.debug(
                    f"Skipping entry {entry_id} with FQN {fqn} - object name '{object_name}' appears to be asset metadata"
                )
                return

    # Track entry for lineage extraction
    with entry_data_lock:
        if project_id not in entry_data_by_project:
            entry_data_by_project[project_id] = set()
        entry_data_by_project[project_id].add(
            EntryDataTuple(
                entry_id=entry_id,
                source_platform=source_platform,
                dataset_id=dataset_id,
            )
        )

    # Generate dataset URN using the full resource path from FQN
    # For BigQuery: bigquery:project.dataset.table -> use full path
    if ":" in fqn:
        _, resource_path = fqn.split(":", 1)
        dataset_name = resource_path
    else:
        dataset_name = entry_id

    dataset_urn = make_dataset_urn_with_platform_instance(
        platform=source_platform,
        name=dataset_name,
        platform_instance=None,
        env=config.env,
    )
    logger.debug(
        f"Created dataset URN for entry {entry_id} (FQN: {fqn}): {dataset_urn}"
    )

    # Extract custom properties using helper method
    custom_properties = extract_entry_custom_properties(entry, entry_id, entry_group_id)

    # Try to extract schema from entry aspects (if enabled)
    schema_metadata = None
    if config.include_schema:
        schema_metadata = extract_schema_from_entry_aspects(
            entry, entry_id, source_platform
        )

    # Build aspects list - extract timestamps and description safely
    created_time = (
        make_audit_stamp(entry.entry_source.create_time)
        if entry.entry_source and entry.entry_source.create_time
        else None
    )
    modified_time = (
        make_audit_stamp(entry.entry_source.update_time)
        if entry.entry_source and entry.entry_source.update_time
        else None
    )
    description = (
        entry.entry_source.description
        if entry.entry_source and entry.entry_source.description
        else ""
    )

    aspects = [
        DatasetPropertiesClass(
            name=entry_id,
            description=description,
            customProperties=custom_properties,
            created=TimeStampClass(**created_time) if created_time else None,
            lastModified=TimeStampClass(**modified_time) if modified_time else None,
        ),
        DataPlatformInstanceClass(platform=str(DataPlatformUrn(source_platform))),
    ]

    # Add schema metadata if available
    if schema_metadata:
        aspects.append(schema_metadata)
        logger.debug(
            f"Added schema metadata for entry {entry_id} with {len(schema_metadata.fields)} fields"
        )

    # Link to source platform container (only for BigQuery)
    if source_platform == "bigquery":
        # Extract project_id and dataset from the full FQN
        # dataset_id format: project.dataset.table
        parts = dataset_id.split(".")
        if len(parts) >= 3:
            bq_project_id = parts[0]
            bq_dataset_id = parts[1]
            with bq_containers_lock:
                container_urn = track_bigquery_container(
                    bq_project_id, bq_dataset_id, bq_containers, config
                )
            if container_urn:
                aspects.append(ContainerClass(container=container_urn))
        else:
            logger.warning(
                f"Could not extract BigQuery project and dataset from dataset_id '{dataset_id}' for entry {entry_id}"
            )

    # Construct MCPs
    yield from construct_mcps_fn(dataset_urn, aspects)
