"""Entity processing utilities for Dataplex source (Lakes/Zones/Entities API)."""

import logging
from collections.abc import Callable
from threading import Lock
from typing import Iterable, Optional

from google.api_core import exceptions
from google.cloud import dataplex_v1

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_containers import (
    track_bigquery_container,
)
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntityDataTuple,
    extract_entity_metadata,
    make_audit_stamp,
    make_entity_dataset_urn,
)
from datahub.ingestion.source.dataplex.dataplex_properties import (
    extract_entity_custom_properties,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.ingestion.source.dataplex.dataplex_schema import extract_schema_metadata
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    SubTypesClass,
    TimeStampClass,
)
from datahub.metadata.urns import DataPlatformUrn

logger = logging.getLogger(__name__)


def process_zone_entities(
    project_id: str,
    lake_id: str,
    zone_id: str,
    config: DataplexConfig,
    report: DataplexReport,
    metadata_client: dataplex_v1.MetadataServiceClient,
    dataplex_client: dataplex_v1.DataplexServiceClient,
    asset_metadata: dict[str, tuple[str, str]],
    asset_metadata_lock: Lock,
    zone_metadata: dict[str, str],
    zone_metadata_lock: Lock,
    entity_data_by_project: dict[str, set[EntityDataTuple]],
    entity_data_lock: Lock,
    bq_containers: dict[str, set[str]],
    bq_containers_lock: Lock,
    report_lock: Lock,
    construct_mcps_fn: Callable[[str, list], Iterable[MetadataChangeProposalWrapper]],
) -> Iterable[MetadataChangeProposalWrapper]:
    """Process all entities for a single zone (called by parallel workers).

    Args:
        project_id: GCP project ID
        lake_id: Dataplex lake ID
        zone_id: Dataplex zone ID
        config: Dataplex configuration object
        report: Report object for tracking statistics
        metadata_client: Dataplex metadata service client
        dataplex_client: Dataplex service client
        asset_metadata: Cache mapping asset names to (platform, dataset_id) tuples
        asset_metadata_lock: Lock for asset_metadata access
        zone_metadata: Cache mapping zone keys to zone types
        zone_metadata_lock: Lock for zone_metadata access
        entity_data_by_project: Mapping of project IDs to entity data tuples
        entity_data_lock: Lock for entity_data_by_project access
        bq_containers: BigQuery containers cache
        bq_containers_lock: Lock for bq_containers access
        report_lock: Lock for report access
        construct_mcps_fn: Function to construct MCPs from dataset URN and aspects

    Yields:
        MetadataChangeProposalWrapper objects for entities in this zone
    """
    entities_parent = f"projects/{project_id}/locations/{config.location}/lakes/{lake_id}/zones/{zone_id}"
    entities_request = dataplex_v1.ListEntitiesRequest(parent=entities_parent)

    try:
        entities = metadata_client.list_entities(request=entities_request)

        for entity in entities:
            entity_id = entity.id
            logger.debug(
                f"Processing entity: {entity_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
            )

            # Skip invalid entities (empty IDs or placeholder metadata)
            if not entity_id or not entity_id.strip():
                logger.debug(
                    f"Skipping entity with empty ID in zone {zone_id}, lake {lake_id}"
                )
                continue

            if not config.filter_config.entities.dataset_pattern.allowed(entity_id):
                logger.debug(
                    f"Entity {entity_id} filtered out by entities.dataset_pattern"
                )
                with report_lock:
                    report.report_entity_scanned(entity_id, filtered=True)
                continue

            with report_lock:
                report.report_entity_scanned(entity_id)
            logger.debug(
                f"Processing entity: {entity_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
            )

            # Determine source platform and dataset id from asset (bigquery, gcs, etc.)
            # Use double-checked locking to avoid holding lock during API calls
            needs_fetch = False
            asset_name = entity.asset
            source_platform: Optional[str] = None
            dataset_id: Optional[str] = None

            with asset_metadata_lock:
                if asset_name in asset_metadata:
                    source_platform, dataset_id = asset_metadata[asset_name]
                else:
                    # Not in cache - need to fetch. Release lock before API call.
                    needs_fetch = True

            if needs_fetch:
                # Make API call WITHOUT holding the lock (allows parallel processing)
                fetched_platform, fetched_dataset_id = extract_entity_metadata(
                    project_id,
                    lake_id,
                    zone_id,
                    entity_id,
                    asset_name,
                    config.location,
                    dataplex_client,
                )

                # Re-acquire lock to update cache
                with asset_metadata_lock:
                    # Check again in case another thread already cached it
                    if asset_name in asset_metadata:
                        source_platform, dataset_id = asset_metadata[asset_name]
                    else:
                        source_platform = fetched_platform
                        dataset_id = fetched_dataset_id
                        if source_platform and dataset_id:
                            asset_metadata[asset_name] = (
                                source_platform,
                                dataset_id,
                            )

            # Skip entities where we couldn't determine platform or dataset
            if source_platform is None or dataset_id is None:
                logger.debug(
                    f"Skipping entity {entity_id} - unable to determine platform or dataset from asset {entity.asset}"
                )
                continue

            # Track entity ID for lineage extraction
            with entity_data_lock:
                if project_id not in entity_data_by_project:
                    entity_data_by_project[project_id] = set()
                entity_data_by_project[project_id].add(
                    EntityDataTuple(
                        lake_id=lake_id,
                        zone_id=zone_id,
                        entity_id=entity_id,
                        asset_id=entity.asset,
                        source_platform=source_platform,
                        dataset_id=dataset_id,
                    )
                )

            # Fetch full entity details including schema
            try:
                get_entity_request = dataplex_v1.GetEntityRequest(
                    name=entity.name,
                    view=dataplex_v1.GetEntityRequest.EntityView.FULL,
                )
                entity_full = metadata_client.get_entity(request=get_entity_request)
            except exceptions.GoogleAPICallError as e:
                logger.warning(
                    f"Could not fetch full entity details for {entity_id}: {e}"
                )
                entity_full = entity

            # Skip non-table entities (only process TABLE and FILESET types)
            entity_type = (
                entity_full.type_.name if hasattr(entity_full, "type_") else None
            )
            if entity_type not in ("TABLE", "FILESET"):
                logger.debug(
                    f"Skipping entity {entity_id} with type {entity_type} - only TABLE and FILESET types are supported"
                )
                continue

            # Skip entities that are just asset metadata (entity_id matches asset name)
            if entity.asset and entity_id == entity.asset:
                logger.debug(
                    f"Skipping entity {entity_id} - entity ID matches asset name, likely asset metadata not a table/file"
                )
                continue

            # Generate dataset URN with source platform (bigquery, gcs, etc.)
            dataset_urn = make_entity_dataset_urn(
                entity_id,
                project_id,
                config.env,
                dataset_id=dataset_id,
                platform=source_platform,
            )

            # Extract schema metadata (if enabled)
            schema_metadata = None
            if config.include_schema:
                schema_metadata = extract_schema_metadata(
                    entity_full, dataset_urn, source_platform
                )

            # Extract custom properties using helper method
            # Access zone_metadata safely with lock
            with zone_metadata_lock:
                zone_metadata_snapshot = dict(zone_metadata)

            custom_properties = extract_entity_custom_properties(
                entity_full,
                project_id,
                lake_id,
                zone_id,
                entity_id,
                zone_metadata_snapshot,
            )

            # Build aspects list
            created_time = make_audit_stamp(entity_full.create_time)
            modified_time = make_audit_stamp(entity_full.update_time)
            aspects = [
                DatasetPropertiesClass(
                    name=entity_id,
                    description=entity_full.description or "",
                    customProperties=custom_properties,
                    created=TimeStampClass(**created_time) if created_time else None,
                    lastModified=TimeStampClass(**modified_time)
                    if modified_time
                    else None,
                ),
                DataPlatformInstanceClass(
                    platform=str(DataPlatformUrn(source_platform))
                ),
                SubTypesClass(
                    typeNames=[
                        entity_full.type_.name,
                    ]
                ),
            ]

            # Add schema metadata if available
            if schema_metadata:
                aspects.append(schema_metadata)

            # Link to source platform container (only for BigQuery)
            if source_platform == "bigquery":
                with bq_containers_lock:
                    container_urn = track_bigquery_container(
                        project_id, dataset_id, bq_containers, config
                    )
                if container_urn:
                    aspects.append(ContainerClass(container=container_urn))

            # Construct MCPs
            yield from construct_mcps_fn(dataset_urn, aspects)

    except exceptions.GoogleAPICallError as e:
        with report_lock:
            report.report_failure(
                title=f"Failed to list entities in zone {zone_id}",
                message=f"Error listing entities in project {project_id}, lake {lake_id}, zone {zone_id}",
                exc=e,
            )
