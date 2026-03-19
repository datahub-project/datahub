"""Entry processing utilities for Dataplex source (Universal Catalog/Entries API)."""

import logging
from collections.abc import Callable
from threading import Lock
from typing import Iterable

from google.cloud import dataplex_v1

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.common.subtypes import DataplexSubTypes
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_containers import (
    track_bigquery_container,
)
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntryDataTuple,
    make_audit_stamp,
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
    SubTypesClass,
    TimeStampClass,
)
from datahub.metadata.urns import DataPlatformUrn

logger = logging.getLogger(__name__)

# Mapping of Google Cloud system types to DataHub platforms
# Based on entry.entry_source.system field from Dataplex Universal Catalog
DATAPLEX_SYSTEM_TO_PLATFORM = {
    "CLOUD_PUBSUB": "pubsub",
    "BIGQUERY": "bigquery",
    "CLOUD_STORAGE": "gcs",
    "CLOUD_BIGTABLE": "bigtable",
    "DATAPROC_METASTORE": "hive",
    "CLOUD_SPANNER": "spanner",
    # Add more mappings as Google Cloud adds support for more services in Dataplex
}

# Mapping of Google Cloud system types to DataHub subtypes
# Based on entry.entry_source.system field from Dataplex Universal Catalog
DATAPLEX_SYSTEM_TO_SUBTYPE = {
    "CLOUD_PUBSUB": DataplexSubTypes.PUBSUB,
    "BIGQUERY": DataplexSubTypes.BIGQUERY,
    "CLOUD_STORAGE": None,  # GCS files don't have a specific subtype
    "CLOUD_BIGTABLE": DataplexSubTypes.BIGTABLE,
    "DATAPROC_METASTORE": DataplexSubTypes.METASTORE,
    "CLOUD_SPANNER": DataplexSubTypes.SPANNER,
    # Add more mappings as Google Cloud adds support for more services in Dataplex
}


def get_datahub_platform(system: str) -> str | None:
    """Get DataHub platform name from Google Cloud system type.

    Args:
        system: Google Cloud system type (e.g., "BIGQUERY", "CLOUD_PUBSUB")

    Returns:
        DataHub platform name (e.g., "bigquery", "pubsub") or None if not mapped
    """
    return DATAPLEX_SYSTEM_TO_PLATFORM.get(system)


def get_datahub_subtype(system: str) -> str | None:
    """Get DataHub subtype from Google Cloud system type.

    Args:
        system: Google Cloud system type (e.g., "BIGQUERY", "CLOUD_PUBSUB")

    Returns:
        DataHub subtype string (e.g., "BigQuery", "Pub/Sub") or None if not mapped
    """
    return DATAPLEX_SYSTEM_TO_SUBTYPE.get(system)


def get_datahub_dataset_id(system: str, fqn: str) -> str:
    """Extract dataset ID from fully qualified name based on system type.

    Args:
        system: Google Cloud system type (e.g., "BIGQUERY", "CLOUD_PUBSUB")
        fqn: Fully qualified name (e.g., 'bigquery:project.dataset.table')

    Returns:
        Dataset ID for the given system type
        - For BigQuery: 'project.dataset.table'
        - For GCS: 'bucket/path'
        - For other systems: resource path after the colon
    """
    if ":" not in fqn:
        # No colon separator, return FQN as-is
        logger.warning(
            f"FQN '{fqn}' does not contain colon separator, using as dataset_id"
        )
        return fqn

    _, resource_path = fqn.split(":", 1)
    return resource_path


def process_entry(
    project_id: str,
    entry: dataplex_v1.Entry,
    entry_group_name: str,
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
        entry_group_name: Entry group name
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

    # Check if entry has FQN
    fqn = entry.fully_qualified_name
    if not fqn:
        logger.warning(f"Entry {entry_id} has no FQN, skipping")
        return

    logger.debug(f"Processing entry {entry_id} with FQN: {fqn}")

    # Get system type from entry source
    system = entry.entry_source.system if entry.entry_source else None
    if not system:
        logger.warning(f"Entry {entry_id} has no system type in entry_source, skipping")
        return

    # Determine platform and dataset_id from system type
    source_platform = get_datahub_platform(system)
    if not source_platform:
        logger.warning(
            f"Unknown system type '{system}' for entry {entry_id} with FQN {fqn}, skipping"
        )
        return

    dataset_id = get_datahub_dataset_id(system, fqn)
    if not dataset_id:
        logger.warning(
            f"Could not extract dataset_id from FQN {fqn} for entry {entry_id}, skipping"
        )
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

    # Get location from entry source (for lineage queries)
    location = (
        entry.entry_source.location
        if entry.entry_source and entry.entry_source.location
        else "global"  # fallback
    )

    # Track entry for lineage extraction
    with entry_data_lock:
        if project_id not in entry_data_by_project:
            entry_data_by_project[project_id] = set()
        entry_data_by_project[project_id].add(
            EntryDataTuple(
                entry_id=entry_id,
                source_platform=source_platform,
                dataset_id=dataset_id,
                location=location,
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
    custom_properties = extract_entry_custom_properties(
        entry, entry_id, entry_group_name
    )

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

    # Add subtypes: "Dataplex" (always) + system-specific subtype (if available)
    subtypes: list[str] = [DataplexSubTypes.DATAPLEX]
    system_subtype = get_datahub_subtype(system)
    if system_subtype:
        subtypes.append(system_subtype)
    aspects.append(SubTypesClass(typeNames=subtypes))
    logger.debug(f"Added subtypes {subtypes} for entry {entry_id} (system: {system})")

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
