"""Helper functions and utilities for Dataplex source."""

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from datahub.emitter.mcp_builder import BigQueryDatasetKey

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntryDataTuple:
    """Immutable data structure for tracking entry metadata.

    Used in sets for lineage extraction, so must be hashable (frozen=True).
    """

    entry_id: str
    source_platform: str
    dataset_id: str


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


def make_audit_stamp(timestamp: Any) -> Optional[Dict[str, Any]]:
    """Create audit stamp from GCP timestamp."""
    if timestamp:
        return {
            "time": int(timestamp.timestamp() * 1000),
            "actor": "urn:li:corpuser:dataplex",
        }
    return None


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
