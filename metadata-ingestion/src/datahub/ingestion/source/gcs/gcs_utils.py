from typing import Optional

GCS_PREFIX = "gs://"


def is_gcs_uri(uri: str) -> bool:
    """
    Check if a URI is a GCS URI (starts with gs://).

    For more general URI handling, consider using object_store.get_object_store_for_uri.
    """
    return uri.startswith(GCS_PREFIX)


def get_gcs_prefix(gcs_uri: str) -> Optional[str]:
    """
    Get the GCS prefix (gs://) if the URI is a GCS URI.

    For more general URI handling, consider using object_store.get_object_store_for_uri.
    """
    if gcs_uri.startswith(GCS_PREFIX):
        return GCS_PREFIX
    return None


def strip_gcs_prefix(gcs_uri: str) -> str:
    """
    Remove the GCS prefix (gs://) from a GCS URI.

    For more general URI handling, consider using the object_store module.

    Args:
        gcs_uri: A GCS URI starting with gs://

    Returns:
        The URI without the gs:// prefix

    Raises:
        ValueError: If the URI doesn't start with gs://
    """
    prefix = get_gcs_prefix(gcs_uri)
    if not prefix:
        raise ValueError(f"Not a GCS URI. Must start with prefix: {GCS_PREFIX}")

    return gcs_uri[len(GCS_PREFIX) :]


def get_gcs_bucket_relative_path(gcs_uri: str) -> str:
    """
    Get the path relative to the bucket from a GCS URI.

    For more general URI handling, consider using object_store.get_object_key.
    """
    return "/".join(strip_gcs_prefix(gcs_uri).split("/")[1:])


def get_gcs_key_prefix(gcs_uri: str) -> str:
    """
    Get the key prefix (first path component after bucket) from a GCS URI.

    For more general URI handling, consider using object_store.get_object_key.
    """
    if not is_gcs_uri(gcs_uri):
        raise ValueError(f"Not a GCS URI. Must start with prefix: {GCS_PREFIX}")
    return strip_gcs_prefix(gcs_uri).split("/", maxsplit=1)[1]
