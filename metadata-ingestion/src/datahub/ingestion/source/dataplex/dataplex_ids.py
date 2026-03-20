"""ID and name mapping utilities for Dataplex source.

FQN Format Reference:
https://docs.cloud.google.com/dataplex/docs/fully-qualified-names
This is the source of truth for all FQN patterns and mappings.
"""

import logging
import re
from typing import Pattern

from datahub.ingestion.source.common.subtypes import DataplexSubTypes

logger = logging.getLogger(__name__)

# Mapping of Google Cloud system types to DataHub platforms
# Based on entry.entry_source.system field from Dataplex Universal Catalog
# Reference: https://docs.cloud.google.com/dataplex/docs/fully-qualified-names
DATAPLEX_SYSTEM_TO_PLATFORM = {
    "CLOUD_PUBSUB": "pubsub",
    "BIGQUERY": "bigquery",
    "CLOUD_STORAGE": "gcs",
    "CLOUD_BIGTABLE": "bigtable",
    "DATAPROC_METASTORE": "hive",
    "CLOUD_SPANNER": "spanner",
    # Add more mappings as we add support for more services in Dataplex
}

# Mapping of Google Cloud system types to DataHub subtypes
# Based on entry.entry_source.system field from Dataplex Universal Catalog
DATAPLEX_SYSTEM_TO_SUBTYPE = {
    "CLOUD_PUBSUB": DataplexSubTypes.TOPIC,
    "BIGQUERY": DataplexSubTypes.TABLE,
    "CLOUD_BIGTABLE": DataplexSubTypes.TABLE,
    "DATAPROC_METASTORE": DataplexSubTypes.TABLE,
    "CLOUD_SPANNER": DataplexSubTypes.TABLE,
    # Add more mappings as we add support for more services in Dataplex
}

# Mapping of platform and subtype to FQN regex pattern
# The regex validates the structure and extracts the dataset ID in one capture group
# Reference: https://docs.cloud.google.com/dataplex/docs/fully-qualified-names
# Structure: {platform: {subtype: Pattern}}
PLATFORM_SUBTYPE_FQN_PATTERNS: dict[str, dict[str, Pattern[str]]] = {
    "bigquery": {
        # BigQuery table: bigquery:{projectId}.{datasetId}.{tableId}
        # Format: project.dataset.table (3 parts)
        "table": re.compile(r"^bigquery:([^.]+\.[^.]+\.[^.]+)$"),
        # BigQuery view: bigquery:{projectId}.{datasetId}.{viewId}
        # Format: project.dataset.view (3 parts)
        "view": re.compile(r"^bigquery:([^.]+\.[^.]+\.[^.]+)$"),
    },
    "bigtable": {
        # Bigtable table: bigtable:{projectId}.{instanceId}.{tableId}
        # Format: project.instance.table (3 parts)
        "table": re.compile(r"^bigtable:([^.]+\.[^.]+\.[^.]+)$"),
    },
    "spanner": {
        # Spanner table: spanner:{projectId}.{instanceConfigId}.{instanceId}.{databaseId}.{tableId}
        # Format: project.instanceConfig.instance.database.table (5 parts)
        "table": re.compile(r"^spanner:([^.]+\.[^.]+\.[^.]+\.[^.]+\.[^.]+)$"),
        # Spanner view: spanner:{projectId}.{instanceConfigId}.{instanceId}.{databaseId}.{viewId}
        # Format: project.instanceConfig.instance.database.view (5 parts)
        "view": re.compile(r"^spanner:([^.]+\.[^.]+\.[^.]+\.[^.]+\.[^.]+)$"),
    },
}

# Legacy mapping for backward compatibility with platforms that don't have subtype-specific patterns
# This includes pubsub, gcs, and hive/metastore
PLATFORM_FQN_PATTERNS: dict[str, Pattern[str]] = {
    # Pub/Sub: pubsub:topic:{projectId}.{topicId}
    #          pubsub:subscription:{projectId}.{subscriptionId}
    # Format: project.topic_or_subscription (2 parts)
    "pubsub": re.compile(r"^pubsub:(?:topic|subscription):([^.]+\.[^.]+)$"),
    # Cloud Storage: gcs:{bucketName}.{virtualPath}
    # Format: bucket.path (2+ parts, bucket alone is a container, not a dataset)
    # Note: Google uses dots to separate bucket and path in FQNs
    "gcs": re.compile(r"^gcs:([^.]+\..+)$"),
    # Dataproc Metastore: dataproc_metastore:{projectId}.{location}.{instanceId}.{databaseId}.{tableId}
    # Format: project.location.instance.database.table (5 parts)
    "hive": re.compile(r"^dataproc_metastore:([^.]+\.[^.]+\.[^.]+\.[^.]+\.[^.]+)$"),
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


def extract_project_id_from_resource(resource: str | None) -> str | None:
    """
    Extract project ID from entry_source.resource field.

    Format: "projects/{project_id}/..."

    Args:
        resource: Resource string from entry_source.resource

    Returns:
        Project ID or None if not found
    """
    if not resource:
        return None
    match = re.match(r"^projects/([^/]+)/", resource)
    if match:
        return match.group(1)
    return None


def get_dataset_id_from_fqn(
    fqn: str,
    system: str,
    project_id: str,
    subtype: str | None = None,
) -> str | None:
    """
    Extract DataHub dataset ID from Dataplex FQN.

    Returns the final dataset ID ready for DataHub URN construction.
    Uses regex patterns to extract the dataset identifier from the FQN,
    then validates the structure.

    Examples:
        - BigQuery table: "bigquery:project.dataset.table" -> "project.dataset.table"
        - BigQuery view: "bigquery:project.dataset.view" -> "project.dataset.view"
        - Pub/Sub: "pubsub:topic:project.topic_name" -> "project.topic_name"
        - GCS: "gcs:bucket/path" -> "bucket/path"

    Args:
        fqn: Fully qualified name from Dataplex
        system: Google Cloud system type (e.g., "CLOUD_PUBSUB")
        project_id: GCP project ID from entry object
        subtype: Optional subtype (e.g., "table", "view") for more specific pattern matching

    Returns:
        Dataset ID ready for DataHub URN, or None if validation fails
    """
    # Get platform from system
    platform = get_datahub_platform(system)
    if not platform:
        logger.warning(f"Unknown system type '{system}' for FQN {fqn}")
        return None

    # Try subtype-specific patterns first if platform has them
    if platform in PLATFORM_SUBTYPE_FQN_PATTERNS:
        subtype_patterns = PLATFORM_SUBTYPE_FQN_PATTERNS[platform]

        # If subtype is specified, try that pattern first
        if subtype and subtype in subtype_patterns:
            pattern = subtype_patterns[subtype]
            match = pattern.match(fqn)
            if match:
                dataset_id = match.group(1)
                _validate_project_id(fqn, dataset_id, project_id)
                return dataset_id

        # Try all subtype patterns for this platform
        for pattern in subtype_patterns.values():
            match = pattern.match(fqn)
            if match:
                dataset_id = match.group(1)
                _validate_project_id(fqn, dataset_id, project_id)
                return dataset_id

        logger.warning(
            f"FQN '{fqn}' does not match any subtype pattern for platform '{platform}'"
        )
        return None

    # Fall back to legacy platform patterns
    if platform not in PLATFORM_FQN_PATTERNS:
        logger.warning(f"No FQN pattern defined for platform '{platform}', FQN: {fqn}")
        return None

    pattern = PLATFORM_FQN_PATTERNS[platform]

    # Extract dataset ID using regex
    match = pattern.match(fqn)
    if not match:
        logger.warning(
            f"FQN '{fqn}' does not match expected pattern for platform '{platform}'"
        )
        return None

    dataset_id = match.group(1)
    _validate_project_id(fqn, dataset_id, project_id)
    return dataset_id


def infer_dataset_id_from_fqn(fqn: str) -> str | None:
    """Infer dataset_id from FQN without prior knowledge of system/project.

    Extracts platform from FQN and attempts to match against known patterns.
    This is used for lineage FQNs where we don't have full entry context
    (system type, project_id, etc.).

    Unlike get_dataset_id_from_fqn(), this function:
    - Does NOT require system, project_id, or subtype
    - Infers the platform from the FQN itself
    - Tries pattern matching with validation
    - Falls back to simple extraction if no pattern matches

    Examples:
        >>> infer_dataset_id_from_fqn("bigquery:proj.dataset.table")
        "proj.dataset.table"
        >>> infer_dataset_id_from_fqn("gcs:bucket/path/file")
        "bucket/path/file"
        >>> infer_dataset_id_from_fqn("unknown:some-identifier")
        "some-identifier"

    Args:
        fqn: Fully qualified name in format "{platform}:{identifier}"

    Returns:
        Dataset ID (everything after platform prefix) or None if extraction fails
    """
    if not fqn or ":" not in fqn:
        logger.warning(f"Invalid FQN format (missing colon): {fqn}")
        return None

    try:
        # Extract platform (left side of first colon)
        platform, dataset_id_candidate = fqn.split(":", 1)

        # Try subtype-specific patterns first (these validate the format)
        if platform in PLATFORM_SUBTYPE_FQN_PATTERNS:
            for pattern in PLATFORM_SUBTYPE_FQN_PATTERNS[platform].values():
                match = pattern.match(fqn)
                if match:
                    return match.group(1)

        # Try legacy platform patterns
        if platform in PLATFORM_FQN_PATTERNS:
            pattern = PLATFORM_FQN_PATTERNS[platform]
            match = pattern.match(fqn)
            if match:
                return match.group(1)

        # If no pattern matches, return the part after colon
        # This handles unknown platforms gracefully
        logger.debug(
            f"No pattern match for platform '{platform}', using simple extraction"
        )
        return dataset_id_candidate

    except Exception as e:
        logger.error(f"Failed to infer dataset_id from FQN '{fqn}': {e}")
        return None


def _validate_project_id(fqn: str, dataset_id: str, project_id: str) -> None:
    """Validate that project ID in dataset_id matches expected project_id."""
    parts = dataset_id.split(".")
    if parts and parts[0] != project_id:
        logger.warning(
            f"Project ID mismatch for FQN '{fqn}': dataset_id has '{parts[0]}', "
            f"entry has '{project_id}'"
        )
        # Continue anyway - use the FQN's project_id
