"""Entry processing utilities for Dataplex source (Universal Catalog/Entries API)."""

import logging
from collections.abc import Callable
from threading import Lock
from typing import Iterable

from google.cloud import dataplex_v1

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_containers import (
    track_bigquery_container,
)
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntityDataTuple,
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

logger = logging.getLogger(__name__)


def process_entry(
    project_id: str,
    entry: dataplex_v1.Entry,
    entry_group_id: str,
    config: DataplexConfig,
    entity_data_by_project: dict[str, set[EntityDataTuple]],
    entity_data_lock: Lock,
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
        entity_data_by_project: Mapping of project IDs to entity data tuples
        entity_data_lock: Lock for entity_data_by_project access
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

    # Track entry for lineage extraction (entries don't have lake/zone/asset info,
    # but lineage API only needs FQN which we can construct from entry metadata)
    with entity_data_lock:
        if project_id not in entity_data_by_project:
            entity_data_by_project[project_id] = set()
        entity_data_by_project[project_id].add(
            EntityDataTuple(
                lake_id="",  # Not available in Entry objects
                zone_id="",  # Not available in Entry objects
                entity_id=entry_id,
                asset_id="",  # Not available in Entry objects
                source_platform=source_platform,
                dataset_id=dataset_id,
                is_entry=True,  # Flag that this is from Entries API
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
