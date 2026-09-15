from typing import Optional

from pydantic import Field, SecretStr

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance

GCS_PREFIX = "gs://"
GCS_ENDPOINT_URL = "https://storage.googleapis.com"


class HMACKey(ConfigModel):
    hmac_access_id: str = Field(description="Access ID")
    hmac_access_secret: SecretStr = Field(description="Secret")


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


def make_gcs_urn(
    gcs_uri: str, env: str, *, platform_instance: Optional[str] = None
) -> str:
    """
    Build the dataset URN that the GCS source itself would emit for this URI.

    Counterpart to `make_s3_urn_for_lineage` and `make_abs_urn`: warehouse sources use
    this to name a GCS dataset the *GCS source* owns, so the name has to be built the way
    gcs/gcs_source.py builds it -- scheme stripped, both ends stripped of slashes, same
    platform instance prefix. Anything else silently produces a dangling upstream instead
    of a connected edge.

    Case is NOT normalized here: the GCS source lowercases via its own
    `convert_urns_to_lowercase` config, which this helper cannot see. Only the global
    DATASET_URN_TO_LOWER flag applies on both sides.
    """
    return make_dataset_urn_with_platform_instance(
        platform="gcs",
        name=strip_gcs_prefix(gcs_uri).strip("/"),
        platform_instance=platform_instance,
        env=env,
    )


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
