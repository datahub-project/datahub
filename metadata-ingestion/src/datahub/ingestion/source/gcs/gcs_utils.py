GCS_PREFIX = "gs://"


def is_gcs_uri(uri: str) -> bool:
    return uri.startswith(GCS_PREFIX)


def strip_gcs_prefix(gcs_uri: str) -> str:
    # remove GCS prefix (gs://)
    if gcs_uri.startswith(GCS_PREFIX):
        plain_base_path = gcs_uri[len(GCS_PREFIX) :]
        return plain_base_path

    raise ValueError(f"Not an GCS URI. Must start with prefix: {GCS_PREFIX}")


def get_gcs_bucket_name(path):
    if not is_gcs_uri(path):
        raise ValueError(f"Not a GCS URI. Must start with prefixe: {GCS_PREFIX}")
    return strip_gcs_prefix(path).split("/")[0]


def get_gcs_bucket_relative_path(gcs_uri: str) -> str:
    return "/".join(strip_gcs_prefix(gcs_uri).split("/")[1:])


def get_gcs_key_prefix(gcs_uri: str) -> str:
    if not is_gcs_uri(gcs_uri):
        raise ValueError(f"Not a GCS URI. Must start with prefixe: {GCS_PREFIX}")
    return strip_gcs_prefix(gcs_uri).split("/", maxsplit=1)[1]
