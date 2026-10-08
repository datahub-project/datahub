import json
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
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

    Deliberately narrower than has_glob_characters, because two shapes carry those
    characters without being patterns, and expanding either one matches nothing -
    which would turn a previously working recipe into a run that quietly ingests
    no assets.

    An HTTP(S) URL's `?` opens its query string, which is where a presigned URL
    carries its signature, so only the URL's path component can make it a pattern.
    And a local file or directory may simply be named with them: `dbt[prod]` is a
    legitimate directory name that fnmatch reads as a character class matching
    nothing, so a path that already resolves literally is read literally.

    The literal-path check does not extend to object stores: os.path.exists is
    always False for an s3:// or gs:// URI, so those keep expanding. Recognising an
    object key that literally contains glob characters would need an existence
    probe against the store per path.
    """
    if is_http_uri(path):
        return has_glob_characters(urlparse(path).path)
    if not has_glob_characters(path):
        return False
    return not os.path.exists(path)


def sibling_artifact_path(manifest_path: str, filename: str) -> str:
    """Resolve an artifact that sits beside the manifest.

    dbt writes manifest.json, catalog.json, and sources.json into a single
    target/ directory, so co-location is dbt's own layout rather than a
    convention we impose. os.path.dirname is used to strip the filename because
    it recognises both separators, so a backslash path from glob.glob on Windows
    resolves as correctly as a POSIX path. The result is always rejoined with a
    forward slash, which every OS accepts and which object-store URIs require.
    """
    prefix = os.path.dirname(manifest_path)
    return f"{prefix}/{filename}" if prefix else filename


_NOT_FOUND_ERROR_CODES = {"NoSuchKey", "NoSuchBucket", "NotFound", "404"}


def is_missing_file_error(err: Optional[BaseException]) -> bool:
    """Whether a failed artifact read definitely means the file is not there.

    A local read raises FileNotFoundError. Object-store reads all surface as the
    same generic ValueError from read_file_as_bytes, but that wrapper preserves the
    original botocore ClientError as __cause__, whose error code separates a
    missing key from a genuinely ambiguous failure (permissions, throttling,
    network). Without this split, an estate where many projects never run
    `dbt docs generate` reports a benign absence as an alarming infrastructure
    fault, once per project, on every run.
    """
    if isinstance(err, FileNotFoundError):
        return True
    response = getattr(getattr(err, "__cause__", None), "response", None)
    if not isinstance(response, dict):
        return False
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in _NOT_FOUND_ERROR_CODES or status == 404


def load_file_as_json(
    uri: str,
    aws_connection: Optional[AwsConnectionConfig],
    gcs_connection: Optional[GCSConnectionConfig] = None,
) -> Dict:
    raw = read_file_as_bytes(uri, aws_connection, gcs_connection)
    # Hand json.loads the raw bytes: it sniffs the BOM and picks UTF-8/16/32
    # accordingly (RFC 4627), matching the old requests.json() behaviour a
    # forced decode("utf-8") had regressed on BOM-prefixed manifests.
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
            context=path,
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


class ArtifactReader:
    """Reads dbt artifacts, optionally ahead of the consumer on a bounded pool.

    `prefetched` holds the bytes (or the exception the read raised) for the
    group currently being consumed, keyed by URI. It is filled and consumed on
    the main thread only.
    """

    def __init__(
        self,
        aws_connection: Optional[AwsConnectionConfig],
        gcs_connection: Optional[GCSConnectionConfig],
        concurrency: int,
    ) -> None:
        self.aws_connection = aws_connection
        self.gcs_connection = gcs_connection
        self.concurrency = concurrency
        self.prefetched: Dict[str, Union[bytes, Exception]] = {}

    def load_json(self, uri: str) -> Dict:
        """Load one artifact, preferring bytes prefetched by prefetch_in_order.

        A prefetched Exception is the exact exception the inline read would have
        raised (workers only capture, they never classify), so re-raising it here
        keeps is_missing_file_error and per-project failure isolation unchanged.
        """
        prefetched = self.prefetched.pop(uri, None)
        if prefetched is None:
            return load_file_as_json(uri, self.aws_connection, self.gcs_connection)
        if isinstance(prefetched, Exception):
            raise prefetched
        # json.loads on raw bytes sniffs the BOM, matching load_file_as_json.
        return json.loads(prefetched)

    def _fetch_group(
        self, index: int, uris: List[str]
    ) -> Tuple[int, Dict[str, Union[bytes, Exception]]]:
        # Runs on a worker thread: fetch only - never parse, classify, or touch
        # self.report. A failed read hands its exception back for the main thread
        # to re-raise at the original call site.
        fetched: Dict[str, Union[bytes, Exception]] = {}
        for uri in uris:
            try:
                fetched[uri] = read_file_as_bytes(
                    uri, self.aws_connection, self.gcs_connection
                )
            except MemoryError:
                # Exhausted memory is systemic, not a per-file failure to capture and
                # replay: let it propagate so .result() re-raises it on the main thread
                # and load_nodes fails instead of skipping the project. Groups already
                # in flight still finish their reads (the executor waits for them on
                # exit), but no further groups are started.
                raise
            except Exception as e:
                fetched[uri] = e
        return index, fetched

    def prefetch_in_order(
        self, uri_groups: List[List[str]], concurrency: int
    ) -> Iterator[Dict[str, Union[bytes, Exception]]]:
        """Fetch each group's files on a bounded pool, yielding groups in input order.

        Submission is windowed to at most `concurrency` groups ahead of the
        consumer and each group is awaited in input order, so the raw bytes held
        in memory stay bounded at ~concurrency groups. A general out-of-order
        executor would instead let a slow early group hold every later group's
        already-fetched bytes in a reorder buffer that grows with the whole run.
        """
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            in_flight: Dict[int, Future] = {}
            next_submit = 0
            for next_index in range(len(uri_groups)):
                while next_submit < len(uri_groups) and len(in_flight) < concurrency:
                    in_flight[next_submit] = executor.submit(
                        self._fetch_group,
                        next_submit,
                        uri_groups[next_submit],
                    )
                    next_submit += 1
                _, fetched = in_flight.pop(next_index).result()
                yield fetched

    def maybe_prefetch(
        self, uri_groups: List[List[str]]
    ) -> Optional[Iterator[Dict[str, Union[bytes, Exception]]]]:
        concurrency = min(self.concurrency, len(uri_groups))
        if concurrency <= 1:
            return None
        return self.prefetch_in_order(uri_groups, concurrency)

    def load_optional_json(
        self, path: Optional[str], *, optional: bool
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Exception]]:
        """Load catalog.json or sources.json, tolerating absence when optional.

        Returns (json, None) if path is None or the load succeeded. If the load
        fails, returns (None, exception) when optional is True (a
        glob-derived sibling guess) and re-raises when False (an
        explicitly-configured path is a real misconfiguration). A file that
        exists but cannot be decoded or parsed always raises either way - only
        "not found" is ever treated as absence. The caught exception is handed back so the
        caller can classify it with is_missing_file_error.
        """
        if path is None:
            return None, None
        try:
            return self.load_json(path), None
        except (json.JSONDecodeError, UnicodeDecodeError):
            # A file that exists but cannot be decoded is corrupt, never missing.
            # UnicodeDecodeError is a ValueError subclass but not a JSONDecodeError,
            # so without naming it here invalid UTF-8 was caught below and reported
            # as "no catalog file found" - silently ingesting the project with no
            # column metadata.
            raise
        except (OSError, ValueError) as e:
            # OSError, not just FileNotFoundError: a local read also raises
            # PermissionError or IsADirectoryError, and on an object store the
            # identical fault arrives as a ValueError from read_file_as_bytes. Both
            # must reach the caller's warn-and-continue path, or the same fault
            # fails the whole project locally while only warning on S3/GCS.
            if not optional:
                raise
            return None, e
