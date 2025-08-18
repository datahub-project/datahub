"""
URI utilities for external data sources.

This module provides common URI validation and type detection utilities that can be
used across different DataHub connectors for handling external data sources including:
- Cloud storage (S3, GCS, Azure Blob Storage)
- HTTP/HTTPS endpoints
- Local file paths

These utilities complement the object store functionality by providing higher-level
URI classification that includes non-cloud sources.
"""

from typing import Optional

from datahub.ingestion.source.data_lake_common.connections import (
    get_data_lake_uri_type,
    is_data_lake_uri,
)


def is_http_uri(uri: str) -> bool:
    """
    Check if a URI is an HTTP/HTTPS URL (excluding cloud storage URLs).

    This function specifically excludes cloud storage URLs that use HTTP/HTTPS
    protocols (like Azure Blob Storage) to avoid classification conflicts.

    Args:
        uri: The URI to check

    Returns:
        bool: True if the URI is an HTTP/HTTPS URL but NOT a cloud storage URL, False otherwise
    """
    if not uri:
        return False

    # First check if it's an HTTP/HTTPS URL
    if not uri.startswith(("http://", "https://")):
        return False

    # Exclude cloud storage URLs that use HTTP/HTTPS protocols
    # Check if it's actually a data lake URI (S3, GCS, Azure)
    if is_data_lake_uri(uri):
        return False

    # It's an HTTP/HTTPS URL but not a cloud storage URL
    return True


def is_external_uri(uri: str) -> bool:
    """
    Check if a URI is for an external source (cloud storage or HTTP).

    This function combines cloud storage detection with HTTP detection to provide
    a unified check for all external (non-local) data sources.

    Args:
        uri: The URI to check

    Returns:
        bool: True if the URI is for an external source, False otherwise
    """
    if not uri:
        return False

    return is_data_lake_uri(uri) or is_http_uri(uri)


def get_external_uri_type(uri: str) -> Optional[str]:
    """
    Get the type of external URI.

    Args:
        uri: The URI to check

    Returns:
        str: The URI type ('s3', 'gcs', 'azure', 'http') or None if not an external URI
    """
    if not uri:
        return None

    # Check cloud storage first
    cloud_type = get_data_lake_uri_type(uri)
    if cloud_type:
        return cloud_type
    elif is_http_uri(uri):
        return "http"
    else:
        return None
