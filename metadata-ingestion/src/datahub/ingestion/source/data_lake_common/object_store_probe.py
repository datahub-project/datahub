"""Probe listings for a data lake reached through the S3 API (S3, and GCS via
its S3-compatible endpoint).

Every listing goes through the same s3_boto_utils helpers ingestion uses and
issues only ListBuckets / ListObjectsV2 -- never GetObject, so no object
content and no schema inference. Listings are lazy paginators cut with islice,
so a limit bounds the requests made, not just what is returned.
"""

import re
from contextlib import contextmanager
from itertools import islice
from typing import (
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Sequence,
    TypeVar,
)

from botocore.exceptions import ClientError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_boto_utils import (
    list_buckets,
    list_folders_path,
    list_objects_recursive_path,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.source import (
    listing_prefix,
    resolve_templated_folders,
    table_marker_prefix,
)

# Folders visited while expanding the wildcards of one include. Ingestion
# expands them all; a probe must not, because `*/*/{table}` over a large
# bucket can list for a long time before yielding a single table.
MAX_RESOLVED_PREFIXES = 1000

_BUCKET_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")
_AUTH_ERROR_CODES = frozenset(
    {"SignatureDoesNotMatch", "InvalidAccessKeyId", "InvalidSecurity", "ExpiredToken"}
)

T = TypeVar("T")


class S3CompatibleMetadataProbe:
    display_scheme: ClassVar[str] = "s3://"

    def __init__(
        self, aws_config: AwsConnectionConfig, path_specs: Sequence[PathSpec]
    ) -> None:
        self._aws_config = aws_config
        # The s3:// specs ingestion matches with, not the recipe's gs:// ones.
        self._path_specs = list(path_specs)
        self.warnings: List[str] = []
        self.failures: List[str] = []

    def __enter__(self) -> "S3CompatibleMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._aws_config.close_cached_s3_clients()

    def _display(self, s3_uri: str) -> str:
        return self.display_scheme + s3_uri[len("s3://") :]

    def _bucket_uri(self, bucket: str, prefix: str) -> str:
        if not _BUCKET_NAME.match(bucket):
            raise ValueError(f"'{bucket}' is not a bucket name")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return f"s3://{bucket}/{prefix.lstrip('/')}"

    @contextmanager
    def _storage_errors(self, context: str, whole: bool) -> Iterator[None]:
        """Split storage errors the way the probe contract asks: a missing bucket
        is the caller's mistake, a denied listing is recorded (as a failure when
        it is the whole answer), a rejected credential is a connection error."""
        try:
            yield
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == "NoSuchBucket":
                raise ValueError(f"{context}: no such bucket") from exc
            if code in _AUTH_ERROR_CODES or status == 401:
                raise ProbeConnectionError(
                    f"{context}: credential rejected ({code})"
                ) from exc
            if code == "AccessDenied":
                message = f"{context}: access denied, so this could not be listed"
                (self.failures if whole else self.warnings).append(message)
                return
            raise

    def _take(
        self, listing: Callable[[], Iterable[T]], limit: int, context: str
    ) -> List[T]:
        # A factory, not an iterable: the listing is created inside the error
        # context, so a helper that raises on the call rather than on the first
        # item is classified too.
        out: List[T] = []
        with self._storage_errors(context, whole=True):
            out.extend(islice(listing(), limit))
        return out

    def _spec(self, index: int) -> PathSpec:
        if not 0 <= index < len(self._path_specs):
            raise ValueError(
                f"path_spec {index} does not exist; the recipe has "
                f"{len(self._path_specs)} (0-based)"
            )
        return self._path_specs[index]

    def _bounded_prefixes(self, prefix: str) -> Iterator[str]:
        for count, resolved in enumerate(
            resolve_templated_folders(prefix, self._aws_config), start=1
        ):
            if count > MAX_RESOLVED_PREFIXES:
                self.warnings.append(
                    f"stopped after resolving {MAX_RESOLVED_PREFIXES} folders for "
                    f"'{self._display(prefix)}'; narrow the include's wildcards "
                    f"to see the rest"
                )
                return
            yield resolved

    def _list_buckets(self, limit: int) -> List[str]:
        return self._take(
            lambda: list_buckets("", self._aws_config), limit, "listing buckets"
        )

    @probe_method(row_limit_param="limit")
    def folders(self, bucket: str, prefix: str = "", limit: int = 200) -> List[str]:
        """Folders one level under bucket/prefix, as full URIs -- the storage
        layout to write a path_spec include against. Lists names only."""
        uri = self._bucket_uri(bucket, prefix)
        entries = self._take(
            lambda: list_folders_path(uri, aws_config=self._aws_config),
            limit,
            f"listing {self._display(uri)}",
        )
        return [self._display(e.path) for e in entries]

    @probe_method(row_limit_param="limit")
    def objects(
        self, bucket: str, prefix: str = "", limit: int = 200
    ) -> List[Dict[str, object]]:
        """Objects under bucket/prefix, recursively: URI, size in bytes and last
        modified time. Use it to check file extensions against a path_spec's
        file_types. Metadata only -- object contents are never read."""
        uri = self._bucket_uri(bucket, prefix)
        found = self._take(
            lambda: list_objects_recursive_path(uri, aws_config=self._aws_config),
            limit,
            f"listing {self._display(uri)}",
        )
        return [
            {
                "name": self._display(f"s3://{o.bucket_name}/{o.key}"),
                "size": o.size,
                "last_modified": o.last_modified.isoformat(),
            }
            for o in found
        ]

    @probe_method(kind=DatasetSubTypes.TABLE, row_limit_param="limit")
    def datasets(self, path_spec: int = 0, limit: int = 200) -> List[Dict[str, str]]:
        """The candidate datasets one path_spec (0-based) resolves to, listed the
        way ingestion lists them: table folders for an include with {table},
        otherwise every object under the include's fixed prefix. Candidates the
        path_spec would drop are reported, not hidden -- judge them with
        `probe filter --kind Table`. Metadata only."""
        spec = self._spec(path_spec)
        if spec.emit_folders_only:
            self.warnings.append(
                f"path_spec {path_spec} sets emit_folders_only and creates no "
                f"datasets; list its folders with path_spec_folders"
            )
            return []
        if "{table}" in spec.include:
            return self._take(
                lambda: self._table_folders(spec), limit, "resolving tables"
            )
        dirname, startswith = listing_prefix(spec.include)
        found = self._take(
            lambda: list_objects_recursive_path(
                dirname, startswith=startswith, aws_config=self._aws_config
            ),
            limit,
            f"listing {self._display(dirname)}",
        )
        return [
            {
                "name": self._display(uri),
                "display_name": spec.extract_table_name_and_path(uri)[0],
            }
            for uri in (f"s3://{o.bucket_name}/{o.key}" for o in found)
        ]

    def _table_folders(self, spec: PathSpec) -> Iterator[Dict[str, str]]:
        # _process_templated_path, stopped at the folder listing: the partition
        # scan after it lists every object in the table and is not needed to
        # name the table.
        for resolved in self._bounded_prefixes(table_marker_prefix(spec.include)):
            with self._storage_errors(
                f"listing {self._display(resolved)}", whole=False
            ):
                for folder in list_folders_path(resolved, aws_config=self._aws_config):
                    yield {
                        "name": self._display(folder.path),
                        "display_name": spec.extract_table_name_and_path(
                            folder.path
                        )[0],
                    }

    @probe_method(kind=DatasetContainerSubTypes.FOLDER, row_limit_param="limit")
    def path_spec_folders(self, path_spec: int = 0, limit: int = 200) -> List[str]:
        """The folders an emit_folders_only path_spec (0-based) walks, including
        ones its exclude or hidden-folder rule would skip -- judge them with
        `probe filter --kind Folder`. Lists names only."""
        spec = self._spec(path_spec)
        if not spec.emit_folders_only:
            self.warnings.append(
                f"path_spec {path_spec} creates datasets, not folders-only "
                f"containers; list them with datasets"
            )
            return []
        found = self._take(
            lambda: self._bounded_prefixes(spec.glob_include),
            limit,
            "resolving folders",
        )
        return [self._display(uri.rstrip("/")) for uri in found]
