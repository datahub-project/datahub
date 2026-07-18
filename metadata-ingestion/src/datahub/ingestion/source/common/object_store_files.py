import fnmatch
import glob as glob_module
import logging
import re
from typing import List, Optional
from urllib.parse import urlparse

import requests

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.common.gcs_connection_config import GCSConnectionConfig
from datahub.ingestion.source.gcs.gcs_utils import is_gcs_uri

logger: logging.Logger = logging.getLogger(__name__)

_GLOB_CHARACTERS = frozenset("*?[]")
_HTTP_URI_PATTERN = re.compile("^https?://")
DEFAULT_HTTP_TIMEOUT_SECONDS = 30


def has_glob_characters(path: str) -> bool:
    return any(c in path for c in _GLOB_CHARACTERS)


def is_http_uri(uri: str) -> bool:
    return bool(_HTTP_URI_PATTERN.match(uri))


def read_file_as_bytes(
    uri: str,
    aws_connection: Optional[AwsConnectionConfig] = None,
    gcs_connection: Optional[GCSConnectionConfig] = None,
    http_timeout_seconds: int = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> bytes:
    """Read a single file from a local path, http(s):// URL, s3:// URI, or gs:// URI.

    Object-store URIs require the matching connection config for credentials.
    """
    if is_http_uri(uri):
        resp = requests.get(uri, timeout=http_timeout_seconds)
        resp.raise_for_status()
        return resp.content
    if is_s3_uri(uri):
        if not aws_connection:
            raise ValueError(f"AWS connection required for S3 URI: {uri}")
        parsed = urlparse(uri)
        try:
            response = aws_connection.get_s3_client().get_object(
                Bucket=parsed.netloc, Key=parsed.path.lstrip("/")
            )
        except Exception as e:
            raise ValueError(f"Failed to read {uri} from object store: {e}") from e
        return response["Body"].read()
    if is_gcs_uri(uri):
        if not gcs_connection:
            raise ValueError(f"GCS connection required for GCS URI: {uri}")
        parsed = urlparse(uri)
        try:
            response = (
                gcs_connection.s3_compatible_connection.get_s3_client().get_object(
                    Bucket=parsed.netloc, Key=parsed.path.lstrip("/")
                )
            )
        except Exception as e:
            raise ValueError(f"Failed to read {uri} from object store: {e}") from e
        return response["Body"].read()
    with open(uri, "rb") as f:
        return f.read()


def expand_object_store_glob(
    uri: str, connection: AwsConnectionConfig, scheme: str
) -> List[str]:
    """Expand a glob pattern over an S3-compatible object store into concrete URIs.

    Lists keys under the longest static prefix of the pattern and matches each
    path segment with fnmatch. `scheme` is the URI scheme to rebuild results
    with (e.g. "s3" or "gs").
    """
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key_pattern = parsed.path.lstrip("/")

    # Use the longest static prefix to limit object store listing scope.
    prefix_parts: List[str] = []
    for part in key_pattern.split("/"):
        if has_glob_characters(part):
            break
        prefix_parts.append(part)
    prefix = "/".join(prefix_parts)
    if prefix:
        prefix += "/"

    s3_client = connection.get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")

    pattern_parts = key_pattern.split("/")
    matched_keys: List[str] = []
    total_scanned = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            total_scanned += 1
            key = obj["Key"]
            key_parts = key.split("/")
            if len(key_parts) == len(pattern_parts) and all(
                fnmatch.fnmatchcase(k, p)
                for k, p in zip(key_parts, pattern_parts, strict=True)
            ):
                matched_keys.append(key)

    logger.info(
        f"{scheme} glob '{uri}': scanned {total_scanned} object(s) under "
        f"prefix '{prefix}', matched {len(matched_keys)}"
    )
    return [f"{scheme}://{bucket}/{key}" for key in sorted(matched_keys)]


def expand_local_glob(pattern: str) -> List[str]:
    return sorted(glob_module.glob(pattern))
