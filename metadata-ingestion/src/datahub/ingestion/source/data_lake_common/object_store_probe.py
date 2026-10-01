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
    Optional,
    Pattern,
    Sequence,
    TypeVar,
)

from botocore.exceptions import BotoCoreError, ClientError, ParamValidationError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig, aws_error_code
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

# Listings one include may cost while its wildcards are expanded: every
# folder listed on the way, plus (for tables) each resolved prefix, which is
# listed once more for its table folders. Ingestion expands them all; a probe
# must not, because `*/*/{table}` over a large bucket can list for a long time
# -- one request per dead-end folder -- before yielding a single table.
MAX_RESOLVED_PREFIXES = 1000


class _ListingBudgetSpent(Exception):
    """Raised from the resolution callback to stop the walk mid-recursion."""

_BUCKET_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")
_AUTH_ERROR_CODES = frozenset(
    {
        "SignatureDoesNotMatch",
        "InvalidAccessKeyId",
        "InvalidSecurity",
        "ExpiredToken",
        "InvalidToken",
        "TokenRefreshRequired",
        "InvalidClientTokenId",
        "ExpiredTokenException",
    }
)
# AwsConnectionConfig resolves credentials -- and assumes aws_role -- inside
# get_session, which first runs on the first listing. STS refusing the role
# rejects the credential; it does not deny the listing.
_CREDENTIAL_OPERATIONS = frozenset(
    {
        "AssumeRole",
        "AssumeRoleWithWebIdentity",
        "AssumeRoleWithSAML",
        "GetCallerIdentity",
    }
)
# Seen with a custom endpoint, where boto's S3 region redirector does not run.
_REGION_ERROR_CODES = frozenset(
    {
        "PermanentRedirect",
        "AuthorizationHeaderMalformed",
        "IllegalLocationConstraintException",
    }
)

T = TypeVar("T")


class S3CompatibleMetadataProbe:
    display_scheme: ClassVar[str] = "s3://"
    bucket_name_pattern: ClassVar[Pattern[str]] = _BUCKET_NAME

    def __init__(
        self,
        aws_config: AwsConnectionConfig,
        path_specs: Sequence[PathSpec],
        refusal: Optional[str] = None,
    ) -> None:
        self._aws_config = aws_config
        # The s3:// specs ingestion matches with, not the recipe's gs:// ones.
        self._path_specs = list(path_specs)
        # Why this recipe cannot be probed at all. Raised as a caller error by
        # every command, not from for_config, which the framework reports as
        # "could not open the source".
        self._refusal = refusal
        self.warnings: List[str] = []
        self.failures: List[str] = []

    def _refuse_if_unavailable(self) -> None:
        if self._refusal is not None:
            raise ValueError(self._refusal)

    def __enter__(self) -> "S3CompatibleMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._aws_config.close_cached_s3_clients()

    def _display(self, s3_uri: str) -> str:
        return self.display_scheme + s3_uri[len("s3://") :]

    def _bucket_uri(self, bucket: str, prefix: str) -> str:
        self._refuse_if_unavailable()
        if not self.bucket_name_pattern.match(bucket):
            raise ValueError(f"'{bucket}' is not a bucket name")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return f"s3://{bucket}/{prefix.lstrip('/')}"

    @contextmanager
    def _storage_errors(self, context: str, whole: bool) -> Iterator[None]:
        """Split storage errors the way the probe contract asks: a missing bucket
        or key is the caller's mistake, a denied listing is recorded (as a
        failure when it is the whole answer), a rejected credential or an
        unreachable endpoint is a connection error.

        Only an error code reaches a message: AWS error text names account ids,
        principals and ARNs, and botocore's client errors name the endpoint host
        or profile.
        """
        try:
            yield
        except ClientError as exc:
            code = aws_error_code(exc)
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if (
                exc.operation_name in _CREDENTIAL_OPERATIONS
                or code in _AUTH_ERROR_CODES
                or status == 401
            ):
                raise ProbeConnectionError(
                    f"{context}: credential rejected ({code or status})"
                ) from exc
            if code == "NoSuchBucket":
                raise ValueError(f"{context}: no such bucket") from exc
            if code == "NoSuchKey":
                raise ValueError(f"{context}: no such object") from exc
            if code == "AccessDenied" or code in _REGION_ERROR_CODES:
                why = (
                    "access denied"
                    if code == "AccessDenied"
                    else "the bucket is in another region than the client; set "
                    "aws_region"
                )
                (self.failures if whole else self.warnings).append(
                    f"{context}: {why}, so this could not be listed"
                )
                return
            raise ProbeConnectionError(
                f"{context}: the storage request failed ({code or status})"
            ) from exc
        except ParamValidationError as exc:
            raise ValueError(
                f"{context}: the request was refused before it was sent "
                f"({aws_error_code(exc)})"
            ) from exc
        except BotoCoreError as exc:
            raise ProbeConnectionError(
                f"{context}: could not reach storage or load credentials "
                f"({aws_error_code(exc)})"
            ) from exc

    def _take(
        self, listing: Callable[[], Iterable[T]], limit: int, context: str
    ) -> List[T]:
        self._refuse_if_unavailable()
        # A factory, not an iterable: the listing is created inside the error
        # context, so a helper that raises on the call rather than on the first
        # item is classified too.
        out: List[T] = []
        with self._storage_errors(context, whole=True):
            out.extend(islice(listing(), limit))
        return out

    def _spec(self, index: int) -> PathSpec:
        self._refuse_if_unavailable()
        if not 0 <= index < len(self._path_specs):
            raise ValueError(
                f"path_spec {index} does not exist; the recipe has "
                f"{len(self._path_specs)} (0-based)"
            )
        return self._path_specs[index]

    def _bounded_prefixes(self, prefix: str, listed_after: bool) -> Iterator[str]:
        """resolve_templated_folders, stopped once it has cost
        MAX_RESOLVED_PREFIXES listings. `listed_after`: the caller lists each
        resolved prefix again, so each one is charged too."""
        spent = 0

        def charge(_: str) -> None:
            nonlocal spent
            spent += 1
            if spent > MAX_RESOLVED_PREFIXES:
                raise _ListingBudgetSpent()

        try:
            for resolved in resolve_templated_folders(
                prefix, self._aws_config, on_listing=charge
            ):
                if listed_after:
                    charge(resolved)
                yield resolved
        except _ListingBudgetSpent:
            self.warnings.append(
                f"stopped after {MAX_RESOLVED_PREFIXES} listings while resolving "
                f"'{self._display(prefix)}', so this answer may be incomplete; "
                f"narrow the include's wildcards to see the rest"
            )

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
        prefix = table_marker_prefix(spec.include)
        # With no wildcard before {table} there is one prefix, and a denied
        # listing of it is the whole answer, as it is for `objects`.
        whole = "*" not in prefix
        for resolved in self._bounded_prefixes(prefix, listed_after=True):
            with self._storage_errors(
                f"listing {self._display(resolved)}", whole=whole
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
            lambda: self._bounded_prefixes(spec.glob_include, listed_after=False),
            limit,
            "resolving folders",
        )
        return [self._display(uri.rstrip("/")) for uri in found]
