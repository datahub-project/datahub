from typing import Optional

from datahub.ingestion.source.data_lake_common.connections import (
    get_data_lake_uri_type,
    is_data_lake_uri,
)


def is_http_uri(uri: str) -> bool:
    """Check if a URI is an HTTP/HTTPS URL (excluding cloud storage URLs)."""
    if not uri:
        return False

    if not uri.startswith(("http://", "https://")):
        return False
    if is_data_lake_uri(uri):
        return False
    return True


def is_external_uri(uri: str) -> bool:
    """Check if a URI is for an external source (cloud storage or HTTP)."""
    if not uri:
        return False

    return is_data_lake_uri(uri) or is_http_uri(uri)


def get_external_uri_type(uri: str) -> Optional[str]:
    """Get the type of external URI ('s3', 'gcs', 'azure', 'http')."""
    if not uri:
        return None
    cloud_type = get_data_lake_uri_type(uri)
    if cloud_type:
        return cloud_type
    elif is_http_uri(uri):
        return "http"
    return None
