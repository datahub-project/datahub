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
from datahub.ingestion.source.dataplex.dataplex_ids import (
    extract_project_id_from_resource,
    get_datahub_platform,
    get_datahub_subtype,
    get_dataset_id_from_fqn,
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


def _is_dataplex_internal_metadata(entry_id: str) -> bool:
    """Check if entry is Dataplex internal metadata (zone/asset tables).

    Dataplex creates internal metadata tables that should not be ingested:
    - Zone metadata tables: entries ending with "_zone" or equal to "zone" followed by digit(s)
    - Asset metadata tables: entries ending with "_asset" or equal to "asset" followed by digit(s)

    Args:
        entry_id: Entry identifier (last part of entry name)

    Returns:
        True if entry is internal metadata, False otherwise
    """
    # Check for zone metadata patterns
    if entry_id.endswith("_zone") or (
        entry_id.startswith("zone") and entry_id[4:].isdigit()
    ):
        return True

    # Check for asset metadata patterns
    if entry_id.endswith("_asset") or (
        entry_id.startswith("asset") and entry_id[5:].isdigit()
    ):
        return True

    return False


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

    # Extract project ID from entry resource
    entry_project_id = extract_project_id_from_resource(
        entry.entry_source.resource if entry.entry_source else ""
    )
    if not entry_project_id:
        logger.warning(f"Could not extract project_id from entry {entry_id}, skipping")
        return

    # Get platform from system type
    source_platform = get_datahub_platform(system)
    if not source_platform:
        logger.warning(
            f"Unknown system type '{system}' for entry {entry_id} with FQN {fqn}, skipping"
        )
        return

    # Skip Dataplex internal metadata tables (zone/asset metadata)
    # These are internal tables created by Dataplex and should not be ingested
    if _is_dataplex_internal_metadata(entry_id):
        logger.debug(
            f"Skipping Dataplex internal metadata entry {entry_id} (FQN: {fqn})"
        )
        return

    # Extract and validate dataset ID from FQN
    dataset_id = get_dataset_id_from_fqn(fqn, system, entry_project_id)
    if not dataset_id:
        # Warning already logged in get_dataset_id_from_fqn
        return

    # Get location from entry source (for lineage queries)
    location = (
        entry.entry_source.location
        if entry.entry_source and entry.entry_source.location
        else "global"  # fallback
    )

    # Track entry for lineage extraction
    with entry_data_lock:
        if entry_project_id not in entry_data_by_project:
            entry_data_by_project[entry_project_id] = set()
        entry_data_by_project[entry_project_id].add(
            EntryDataTuple(
                source_platform=source_platform,
                dataset_id=dataset_id,
                location=location,
                project_id=entry_project_id,
                fqn=fqn,
            )
        )

    # Generate dataset URN using dataset_id from FQN
    dataset_urn = make_dataset_urn_with_platform_instance(
        platform=source_platform,
        name=dataset_id,
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
