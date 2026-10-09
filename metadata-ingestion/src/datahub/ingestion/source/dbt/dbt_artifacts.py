import json
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, Generator, List, Optional, Union
from urllib.parse import urlparse

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.common.gcs_connection_config import GCSConnectionConfig
from datahub.ingestion.source.common.object_store_files import (
    expand_local_glob,
    expand_object_store_glob,
    has_glob_characters,
    is_http_uri,
    read_file_as_bytes,
)
from datahub.ingestion.source.dbt.dbt_common import DBTSourceReport
from datahub.ingestion.source.gcs.gcs_utils import is_gcs_uri

logger = logging.getLogger(__name__)


def is_glob_pattern(path: str) -> bool:
    """Whether a configured artifact path should be glob-expanded rather than read as-is.

    Purely syntactic, so the config validator gives the same verdict on every host.
    An HTTP(S) URL's `?` opens its query string, where a presigned URL carries its
    signature, so only the URL's path can make it a pattern. A literal `*?[]` in a
    local or object-store path is escaped as `[*]`, `[?]` or `[[]`.
    """
    if is_http_uri(path):
        return has_glob_characters(urlparse(path).path)
    return has_glob_characters(path)


def redact_url_query(path: str) -> str:
    """Drop an HTTP(S) URL's query string, where a presigned URL carries its signature."""
    if not is_http_uri(path):
        return path
    parsed = urlparse(path)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def sibling_artifact_path(manifest_path: str, filename: str) -> str:
    """Resolve an artifact in the manifest's own target/ directory.

    os.path.dirname handles both separators (Windows glob results); the forward
    slash rejoin works on every OS and is what object-store URIs require.
    """
    prefix = os.path.dirname(manifest_path)
    return f"{prefix}/{filename}" if prefix else filename


def load_file_as_json(
    uri: str,
    aws_connection: Optional[AwsConnectionConfig],
    gcs_connection: Optional[GCSConnectionConfig] = None,
) -> Dict:
    raw = read_file_as_bytes(uri, aws_connection, gcs_connection)
    # json.loads on raw bytes sniffs the BOM and picks UTF-8/16/32 (RFC 4627).
    return json.loads(raw)


def _expand_cloud_glob(
    report: DBTSourceReport,
    path: str,
    connection: Optional[AwsConnectionConfig],
    scheme: str,
    *,
    store_label: str,
    connection_field: str,
) -> List[str]:
    # store_label is the user-facing storage name ("S3"/"GCS"); connection_field
    # is the recipe key that supplies credentials ("aws_connection"/"gcs_connection").
    if not connection:
        report.failure(
            title="Missing cloud connection for glob expansion",
            message="Cloud connection is required for glob pattern",
            context=f"{connection_field}: {path}",
        )
        return []
    try:
        matched_paths = expand_object_store_glob(path, connection, scheme)
    except Exception as e:
        report.failure(
            title="Cloud glob expansion failed",
            message="Failed to expand cloud glob pattern",
            context=f"{store_label}: {path}",
            exc=e,
        )
        return []
    if not matched_paths:
        report.warning(
            title="Cloud glob pattern matched no objects",
            message="Glob pattern did not match any objects",
            context=f"{store_label}: {path}",
        )
    else:
        logger.info(
            f"{store_label} glob pattern '{path}' expanded to "
            f"{len(matched_paths)} file(s)"
        )
    return matched_paths


def expand_glob_path(
    path: str,
    *,
    aws_connection: Optional[AwsConnectionConfig],
    gcs_connection: Optional[GCSConnectionConfig],
    report: DBTSourceReport,
) -> List[str]:
    """Expand a path that may contain glob characters.

    Returns [path] unchanged when there are no glob characters, so callers can
    use this unconditionally. Results are sorted by the caller. Connections and
    the report are arguments because test_connection has a config but no
    source instance and must expand a globbed manifest_path the same way
    ingestion does.
    """
    if not is_glob_pattern(path):
        return [path]

    if is_s3_uri(path):
        return _expand_cloud_glob(
            report,
            path,
            aws_connection,
            "s3",
            store_label="S3",
            connection_field="aws_connection",
        )
    elif is_gcs_uri(path):
        return _expand_cloud_glob(
            report,
            path,
            gcs_connection.s3_compatible_connection if gcs_connection else None,
            "gs",
            store_label="GCS",
            connection_field="gcs_connection",
        )
    elif is_http_uri(path):
        report.warning(
            title="Glob patterns not supported for HTTP(S) URIs",
            message="Glob patterns are not supported for HTTP(S) URIs, please provide explicit file paths",
            context=redact_url_query(path),
        )
        return []
    else:
        local_paths = expand_local_glob(path)
        if not local_paths:
            report.warning(
                title="Local glob pattern matched no files",
                message="Glob pattern did not match any local files",
                context=path,
            )
        else:
            logger.info(
                f"Local glob pattern '{path}' expanded to {len(local_paths)} file(s)"
            )
        return local_paths


Prefetched = Dict[str, Union[bytes, Exception]]


class ArtifactReader:
    """Reads dbt artifacts, optionally ahead of the consumer on a bounded pool."""

    def __init__(
        self,
        aws_connection: Optional[AwsConnectionConfig],
        gcs_connection: Optional[GCSConnectionConfig],
        concurrency: int,
    ) -> None:
        self.aws_connection = aws_connection
        self.gcs_connection = gcs_connection
        self.concurrency = concurrency

    def load_json(self, uri: str, prefetched: Optional[Prefetched] = None) -> Dict:
        """Load one artifact, preferring bytes already fetched by prefetch_in_order.

        A prefetched Exception is the exact exception the inline read would have
        raised (workers only capture, they never classify), so re-raising it here
        keeps error handling identical to an inline read.
        """
        fetched = prefetched.pop(uri, None) if prefetched is not None else None
        if fetched is None:
            return load_file_as_json(uri, self.aws_connection, self.gcs_connection)
        if isinstance(fetched, Exception):
            raise fetched
        # json.loads on raw bytes sniffs the BOM, matching load_file_as_json.
        return json.loads(fetched)

    def _fetch_group(self, uris: List[str]) -> Prefetched:
        # Runs on a worker thread: fetch only, never parse or report. A failed read
        # hands its exception back for the main thread to re-raise at the call site.
        fetched: Prefetched = {}
        for uri in uris:
            try:
                fetched[uri] = read_file_as_bytes(
                    uri, self.aws_connection, self.gcs_connection
                )
            except MemoryError:
                # Systemic, not a per-file failure: .result() re-raises it on the
                # main thread so the run fails instead of skipping the project.
                raise
            except Exception as e:
                fetched[uri] = e
        return fetched

    def prefetch_in_order(
        self, uri_groups: List[List[str]], concurrency: int
    ) -> Generator[Prefetched, None, None]:
        """Fetch each group's files on a bounded pool, yielding groups in input order.

        Submission is windowed to at most `concurrency` groups ahead of the
        consumer and each group is awaited in input order, so the raw bytes held
        in memory stay bounded at ~concurrency groups. A general out-of-order
        executor would instead let a slow early group hold every later group's
        already-fetched bytes in a reorder buffer that grows with the whole run.
        """
        executor = ThreadPoolExecutor(max_workers=concurrency)
        try:
            in_flight: Dict[int, Future] = {}
            next_submit = 0
            for next_index in range(len(uri_groups)):
                while next_submit < len(uri_groups) and len(in_flight) < concurrency:
                    in_flight[next_submit] = executor.submit(
                        self._fetch_group, uri_groups[next_submit]
                    )
                    next_submit += 1
                yield in_flight.pop(next_index).result()
        finally:
            # Runs when the consumer closes the generator early too, so reads that
            # have not started are dropped instead of pinning bytes until GC.
            executor.shutdown(wait=True, cancel_futures=True)

    def maybe_prefetch(
        self, uri_groups: List[List[str]]
    ) -> Optional[Generator[Prefetched, None, None]]:
        concurrency = min(self.concurrency, len(uri_groups))
        if concurrency <= 1:
            return None
        return self.prefetch_in_order(uri_groups, concurrency)

    def load_optional_json(
        self, path: str, prefetched: Optional[Prefetched], *, optional: bool
    ) -> Optional[Dict[str, Any]]:
        """Load catalog.json or sources.json.

        Returns None only when `optional` and the file definitely does not exist.
        Every other failure raises: a corrupt file, and also a read that failed for
        a reason that does not establish absence (permissions, throttling,
        network), since treating that as "no catalog" would overwrite schemas with
        manifest-only columns or, with only_include_if_in_catalog, drop the project.
        """
        try:
            return self.load_json(path, prefetched)
        except FileNotFoundError:
            if not optional:
                raise
            return None
