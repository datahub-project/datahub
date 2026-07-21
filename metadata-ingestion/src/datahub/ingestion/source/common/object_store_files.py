import fnmatch
import glob as glob_module
import logging
import os
import re
from typing import Iterable, List, Optional, Protocol
from urllib.parse import urlparse

import requests

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.common.gcs_connection_config import GCSConnectionConfig
from datahub.ingestion.source.common.http_connection_config import HTTPConnectionConfig
from datahub.ingestion.source.gcs.gcs_utils import is_gcs_uri

logger: logging.Logger = logging.getLogger(__name__)

_GLOB_CHARACTERS = frozenset("*?[]")
_HTTP_URI_PATTERN = re.compile("^https?://")
DEFAULT_HTTP_TIMEOUT_SECONDS = 30
_STREAM_CHUNK_BYTES = 1024 * 1024


def has_glob_characters(path: str) -> bool:
    return any(c in path for c in _GLOB_CHARACTERS)


def is_http_uri(uri: str) -> bool:
    return bool(_HTTP_URI_PATTERN.match(uri))


class FileSizeExceededError(ValueError):
    """A source exceeded the caller's max_bytes cap; callers may treat this as a
    skip rather than a hard read failure."""


def _enforce_size_cap(uri: str, size: int, max_bytes: Optional[int]) -> None:
    if max_bytes is not None and size > max_bytes:
        raise FileSizeExceededError(
            f"{uri} is {size} bytes, over the configured max_bytes limit of {max_bytes}"
        )


def _read_chunks_capped(
    uri: str, chunks: Iterable[bytes], max_bytes: Optional[int]
) -> bytes:
    buffer = bytearray()
    for chunk in chunks:
        buffer.extend(chunk)
        _enforce_size_cap(uri, len(buffer), max_bytes)
    return bytes(buffer)


class _ReadableBody(Protocol):
    def read(self, amt: Optional[int] = ...) -> bytes: ...


def _read_object_store_body(
    uri: str,
    body: _ReadableBody,
    declared_size: Optional[int],
    max_bytes: Optional[int],
) -> bytes:
    if declared_size is not None:
        _enforce_size_cap(uri, declared_size, max_bytes)
    if max_bytes is None:
        return body.read()
    # read at most max_bytes+1 so an oversized object trips the cap without
    # pulling the whole body into memory (a lying/absent ContentLength above).
    data = body.read(max_bytes + 1)
    _enforce_size_cap(uri, len(data), max_bytes)
    return data


def _http_request_kwargs(http_connection: Optional[HTTPConnectionConfig]) -> dict:
    # requests strips the Authorization header on cross-host redirects, so a
    # bearer token / basic auth is not leaked to a redirected origin. Custom
    # header schemes would not get that protection, which is why we only expose
    # bearer + basic here.
    if http_connection is None:
        return {}
    kwargs: dict = {"verify": http_connection.verify_ssl}
    if http_connection.token is not None:
        kwargs["headers"] = {
            "Authorization": f"Bearer {http_connection.token.get_secret_value()}"
        }
    elif http_connection.username is not None and http_connection.password is not None:
        kwargs["auth"] = (
            http_connection.username,
            http_connection.password.get_secret_value(),
        )
    return kwargs


def read_file_as_bytes(
    uri: str,
    aws_connection: Optional[AwsConnectionConfig] = None,
    gcs_connection: Optional[GCSConnectionConfig] = None,
    http_timeout_seconds: int = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_bytes: Optional[int] = None,
    http_connection: Optional[HTTPConnectionConfig] = None,
) -> bytes:
    """Read a single file from a local path, http(s):// URL, s3:// URI, or gs:// URI.

    Object-store URIs require the matching connection config for credentials.
    http(s):// URLs may pass an HTTPConnectionConfig for bearer/basic auth and
    TLS verification. When max_bytes is set the read is bounded — an oversized
    source is rejected from its declared size, or mid-stream, before it is
    fully buffered.
    """
    if is_http_uri(uri):
        # stream=True keeps the body out of memory until we pull it chunk by
        # chunk, so the cap can abort an oversized download partway through.
        # NOTE: redirects are followed (requests default, capped at 30 hops).
        # The URL comes from operator-authored recipe config, not end-user
        # input, so redirect-based SSRF is an accepted risk here.
        # `with` guarantees the connection is released back to the pool even
        # when the size cap trips before the body is drained.
        with requests.get(
            uri,
            timeout=http_timeout_seconds,
            stream=True,
            **_http_request_kwargs(http_connection),
        ) as resp:
            resp.raise_for_status()
            declared = resp.headers.get("Content-Length")
            if declared is not None and declared.isdigit():
                _enforce_size_cap(uri, int(declared), max_bytes)
            return _read_chunks_capped(
                uri, resp.iter_content(chunk_size=_STREAM_CHUNK_BYTES), max_bytes
            )
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
        return _read_object_store_body(
            uri, response["Body"], response.get("ContentLength"), max_bytes
        )
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
        return _read_object_store_body(
            uri, response["Body"], response.get("ContentLength"), max_bytes
        )
    _enforce_size_cap(uri, os.path.getsize(uri), max_bytes)
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
