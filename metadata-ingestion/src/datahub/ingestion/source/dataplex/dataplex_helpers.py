"""Helper functions and utilities for Dataplex source."""

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

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

logger = logging.getLogger(__name__)


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


def make_audit_stamp(timestamp: Any) -> Optional[Dict[str, Any]]:
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
        return SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"]))

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

    # Initialize with default values in case resource_type is not recognized
    platform = None
    dataset_id = None

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


def serialize_field_value(field_value: Any) -> str:
    """Serialize a protobuf field value to string.

    Handles proto MapComposite objects, RepeatedComposite (proto lists), and primitives.

    Args:
        field_value: Value from protobuf message field

    Returns:
        JSON string representation for complex types, string for primitives
    """
    # Handle None
    if field_value is None:
        return ""

    # Get the class name for type checking
    class_name = str(field_value.__class__) if hasattr(field_value, "__class__") else ""

    # Handle proto RepeatedComposite (list-like) objects FIRST
    # This is what contains the list of MapComposite objects
    if "RepeatedComposite" in class_name or "Repeated" in class_name:
        try:
            # RepeatedComposite is iterable, convert items to list
            serializable_list = []
            for item in field_value:
                item_class = str(item.__class__) if hasattr(item, "__class__") else ""
                if "MapComposite" in item_class:
                    # Convert MapComposite to regular dict with primitive values
                    item_dict = {}
                    for key, value in dict(item).items():
                        # Recursively handle nested proto objects
                        if hasattr(value, "__class__") and (
                            "MapComposite" in str(value.__class__)
                            or "RepeatedComposite" in str(value.__class__)
                        ):
                            # Recursively serialize nested proto objects
                            item_dict[key] = json.loads(serialize_field_value(value))
                        else:
                            # Primitives - keep as is
                            item_dict[key] = str(value)
                    serializable_list.append(item_dict)
                else:
                    # Handle primitives in the list
                    serializable_list.append(item)
            return json.dumps(serializable_list)
        except Exception as e:
            logger.warning(
                f"Failed to serialize RepeatedComposite: {e}. Returning length."
            )
            try:
                return f"[{len(list(field_value))} items]"
            except Exception:
                return "[unknown items]"

    # Handle proto MapComposite (dict-like) objects
    if "MapComposite" in class_name:
        try:
            dict_value = dict(field_value)
            return json.dumps(dict_value)
        except Exception as e:
            logger.warning(
                f"Failed to serialize MapComposite to JSON: {e}. Returning type name."
            )
            return str(type(field_value).__name__)

    # Handle regular Python lists/tuples
    if isinstance(field_value, (list, tuple)):
        # Check if it's a list of proto objects
        if field_value and hasattr(field_value[0], "__class__"):
            first_class = str(field_value[0].__class__)
            if "proto" in first_class.lower() or "MapComposite" in first_class:
                # Try to serialize as JSON
                try:
                    # Convert proto objects to dicts if possible
                    serializable_list2: list[Any] = []
                    for item in field_value:
                        if hasattr(item, "__class__") and "MapComposite" in str(
                            item.__class__
                        ):
                            serializable_list2.append(dict(item))
                        else:
                            serializable_list2.append(str(item))
                    return json.dumps(serializable_list2)
                except Exception as e:
                    logger.warning(
                        f"Failed to serialize list of proto objects: {e}. Returning length."
                    )
                    return f"[{len(field_value)} items]"
        # Regular list of primitives
        return json.dumps(field_value)

    # Handle primitives (str, int, float, bool)
    if isinstance(field_value, (str, int, float, bool)):
        return str(field_value)

    # Fallback: try JSON serialization, then string conversion
    try:
        return json.dumps(field_value)
    except (TypeError, ValueError):
        return str(field_value)


def parse_entry_fqn(fqn: str) -> tuple[str, str]:
    """Parse fully qualified name to extract platform and dataset_id.

    Args:
        fqn: Fully qualified name (e.g., 'bigquery:project.dataset.table')

    Returns:
        Tuple of (platform, dataset_id)
        - For BigQuery: dataset_id is 'project.dataset.table'
        - For GCS: dataset_id is 'bucket/path'
    """
    if ":" not in fqn:
        return "", ""

    platform, resource_path = fqn.split(":", 1)

    if platform == "bigquery":
        # BigQuery FQN format: bigquery:project.dataset.table
        # Return the full project.dataset.table as dataset_id
        parts = resource_path.split(".")
        if len(parts) >= 3:
            # Full table reference: project.dataset.table
            return platform, resource_path
        elif len(parts) == 2:
            # Dataset reference (legacy): project.dataset
            logger.warning(
                f"BigQuery FQN '{fqn}' only has 2 parts (project.dataset), expected 3 (project.dataset.table)"
            )
            return platform, resource_path
        else:
            logger.warning(
                f"BigQuery FQN '{fqn}' has unexpected format, expected 'bigquery:project.dataset.table'"
            )
            return platform, resource_path
    elif platform == "gcs":
        # GCS FQN format: gcs:bucket/path
        # Return the full bucket/path as dataset_id
        return platform, resource_path

    # For other platforms, return the full resource_path
    return platform, resource_path
