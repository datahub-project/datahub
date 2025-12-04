"""Helper functions and utilities for Dataplex source."""

from dataclasses import dataclass
from typing import Dict, Optional

from google.api_core import exceptions
from google.cloud import dataplex_v1

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import BigQueryDatasetKey
from datahub.ingestion.source.sql.sql_types import DATAPLEX_TYPES_MAP
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)


@dataclass(frozen=True)
class EntityDataTuple:
    """Immutable data structure for tracking entity metadata.

    Used in sets for lineage extraction, so must be hashable (frozen=True).
    """

    lake_id: str
    zone_id: str
    entity_id: str
    asset_id: str
    source_platform: str
    dataset_id: str
    is_entry: bool = False  # True if from Entries API (use simple naming), False if from Entities API (use hierarchical naming)


def make_bigquery_dataset_container_key(
    project_id: str, dataset_id: str, platform: str, env: str
) -> BigQueryDatasetKey:
    """Create container key for a BigQuery dataset.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        platform: Platform name (should be "bigquery")
        env: Environment (PROD, DEV, etc.)

    Returns:
        BigQueryDatasetKey for the dataset container
    """
    return BigQueryDatasetKey(
        project_id=project_id,
        dataset_id=dataset_id,
        platform=platform,
        env=env,
        backcompat_env_as_instance=True,
    )


def make_entity_dataset_urn(
    entity_id: str,
    project_id: str,
    env: str,
    dataset_id: str,
    platform: str,
) -> str:
    """Create dataset URN for a Dataplex entity using the source platform.

    Args:
        entity_id: The entity ID from Dataplex
        project_id: The GCP project ID
        env: The environment (PROD, DEV, etc.)
        dataset_id: The dataset ID (BigQuery dataset or GCS bucket)
        platform: The source platform (bigquery, gcs, etc.)

    Returns:
        The dataset URN using the source platform
    """
    dataset_name = f"{project_id}.{dataset_id}.{entity_id}"
    return builder.make_dataset_urn_with_platform_instance(
        platform=platform,
        name=dataset_name,
        platform_instance=None,
        env=env,
    )


def make_audit_stamp(timestamp) -> Optional[Dict]:
    """Create audit stamp from GCP timestamp."""
    if timestamp:
        return {
            "time": int(timestamp.timestamp() * 1000),
            "actor": "urn:li:corpuser:dataplex",
        }
    return None


def make_dataplex_external_url(
    resource_type: str, resource_id: str, project_id: str, location: str, base_url: str
) -> str:
    """Generate external URL for Dataplex console."""
    return (
        f"{base_url}/{resource_type}/locations/{location}/"
        f"{resource_type}/{resource_id}?project={project_id}"
    )


def determine_entity_platform(
    entity: dataplex_v1.Entity,
    project_id: str,
    lake_id: str,
    zone_id: str,
    location: str,
    dataplex_client: dataplex_v1.DataplexServiceClient,
) -> str:
    """Determine the platform (bigquery, gcs, dataplex) for an entity based on its asset."""
    platform = "dataplex"

    if not entity.asset:
        return platform

    try:
        asset_id = entity.asset
        asset_name = f"projects/{project_id}/locations/{location}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}"
        asset_request = dataplex_v1.GetAssetRequest(name=asset_name)
        asset = dataplex_client.get_asset(request=asset_request)

        if asset.resource_spec:
            resource_type = asset.resource_spec.type_.name
            if resource_type == "BIGQUERY_DATASET":
                platform = "bigquery"
            elif resource_type == "STORAGE_BUCKET":
                platform = "gcs"
    except exceptions.GoogleAPICallError:
        # Return default platform if we can't determine from asset
        pass
    except AttributeError:
        # Return default platform if asset structure is unexpected
        pass

    return platform


def map_dataplex_type_to_datahub(type_name: str) -> SchemaFieldDataTypeClass:
    """Map Dataplex type name to DataHub schema field data type.

    Uses DATAPLEX_TYPES_MAP from sql_types.py which contains type classes.
    We instantiate them here to get type instances for SchemaFieldDataTypeClass.
    """
    type_class = DATAPLEX_TYPES_MAP.get(type_name)
    # Default to StringType if type is not found
    datahub_type = type_class() if type_class else StringTypeClass()
    return SchemaFieldDataTypeClass(type=datahub_type)


def map_dataplex_field_to_datahub(
    field: dataplex_v1.types.Schema.SchemaField,
) -> SchemaFieldDataTypeClass:
    """Map Dataplex field (with mode) to DataHub field type."""
    type_name = dataplex_v1.types.Schema.Type(field.type_).name
    mode = dataplex_v1.types.Schema.Mode(field.mode).name

    # Handle array types (REPEATED mode)
    if mode == "REPEATED":
        inner_type = map_dataplex_type_to_datahub(type_name)
        return SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=[inner_type]))

    return map_dataplex_type_to_datahub(type_name)


def extract_entity_metadata(
    project_id: str,
    lake_id: str,
    zone_id: str,
    entity_id: str,
    asset_id: str,
    location: str,
    dataplex_client: dataplex_v1.DataplexServiceClient,
) -> tuple[Optional[str], Optional[str]]:
    """Extract entity metadata including platform and dataset_id.

    Args:
        project_id: GCP project ID
        lake_id: Dataplex lake ID
        zone_id: Dataplex zone ID
        entity_id: Entity ID
        asset_id: Asset ID
        location: GCP location
        dataplex_client: Dataplex service client

    Returns:
        Tuple with (platform, dataset_id). Returns (None, None) if extraction fails.
    """

    try:
        asset_name = f"projects/{project_id}/locations/{location}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}"
        asset_request = dataplex_v1.GetAssetRequest(name=asset_name)
        asset = dataplex_client.get_asset(request=asset_request)

        if asset.resource_spec:
            resource_type = asset.resource_spec.type_.name
            resource_name = asset.resource_spec.name

            if resource_type == "BIGQUERY_DATASET":
                platform = "bigquery"
                # Extract dataset_id from resource_name
                # Format: projects/{project}/datasets/{dataset} or just {dataset}
                if resource_name:
                    if "/datasets/" in resource_name:
                        # Extract dataset name from full path
                        dataset_id = resource_name.split("/datasets/")[-1]
                    else:
                        # Assume resource_name is the dataset name
                        dataset_id = resource_name
                else:
                    # Fallback: use zone_id if resource_name is not available
                    dataset_id = zone_id

            elif resource_type == "STORAGE_BUCKET":
                platform = "gcs"
                # For GCS, dataset_id is the bucket name
                if resource_name:
                    # Extract bucket name from resource_name
                    # Format: projects/{project}/buckets/{bucket} or gs://{bucket} or just {bucket}
                    if "/buckets/" in resource_name:
                        dataset_id = resource_name.split("/buckets/")[-1]
                    elif resource_name.startswith("gs://"):
                        dataset_id = resource_name.replace("gs://", "").split("/")[0]
                    else:
                        dataset_id = resource_name
                else:
                    # Fallback: use zone_id if resource_name is not available
                    dataset_id = zone_id

    except exceptions.GoogleAPICallError:
        # Return default values if we can't determine from asset
        return None, None
    except AttributeError:
        # Return default values if asset structure is unexpected
        return None, None

    return platform, dataset_id
