"""Probe provider for the AWS Glue Data Catalog: databases, tables and jobs.

Metadata only. Every record is an allowlist projection of the Glue API shape:
Glue keeps free-form parameters on databases, tables and columns, and job
arguments that routinely carry connection passwords, and register_secrets only
masks secrets that came from the recipe.
"""

import itertools
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    NoRegionError,
    PartialCredentialsError,
)

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError, ProbeSoftError
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws.glue import (
    GlueSource,
    GlueSourceConfig,
    GlueSourceReport,
    glue_catalog_kwargs,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient

_T = TypeVar("_T")

# A refusal of one call by IAM or Lake Formation. Glue says
# AccessDeniedException; STS (aws_role) says AccessDenied.
_ACCESS_DENIED_CODES = frozenset({"AccessDeniedException", "AccessDenied"})
# The request was not accepted as coming from valid credentials at all.
_AUTH_CODES = frozenset(
    {
        "UnrecognizedClientException",
        "InvalidClientTokenId",
        "ExpiredTokenException",
        "ExpiredToken",
        "InvalidSignatureException",
        "SignatureDoesNotMatch",
        "IncompleteSignature",
        "MissingAuthenticationTokenException",
    }
)


def _request_id_suffix(exc: ClientError) -> str:
    request_id = exc.response.get("ResponseMetadata", {}).get("RequestId")
    return f" (AWS request id {request_id})" if request_id else ""


def _translated(
    exc: Union[ClientError, BotoCoreError], action: str, soft_on_denied: bool
) -> Exception:
    """The probe's own exception for one failed AWS call.

    Worded here, never from str(exc). A ClientError renders as "An error
    occurred (Code) when calling the Op operation: <message>", and for an
    authorization failure the message names the calling principal's ARN: the
    account id, the role, and for assumed roles and SSO a session name that is
    often a person's email. botocore keeps key material out of its own text
    (it masks proxy userinfo, and the request id lives in ResponseMetadata),
    but an identity is not ours to print. The code, the action and the
    request id are enough to diagnose with.
    """
    if isinstance(exc, NoRegionError):
        return ValueError(
            "no AWS region was resolved for Glue; set aws_region in the recipe"
        )
    if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
        return ProbeConnectionError(
            f"no usable AWS credentials were found for {action} "
            f"({type(exc).__name__}); set aws_access_key_id and "
            f"aws_secret_access_key, aws_profile or aws_role, or run where the "
            f"default AWS credential chain resolves"
        )
    if isinstance(exc, ClientError):
        code = aws_error_code(exc) or "unknown error"
        suffix = _request_id_suffix(exc)
        if code in _ACCESS_DENIED_CODES:
            message = (
                f"{action} was denied ({code}){suffix}; the recipe's AWS "
                f"principal needs this IAM permission, and Lake Formation "
                f"permission where Lake Formation governs the catalog"
            )
            if soft_on_denied:
                return ProbeSoftError(message)
            return ProbeConnectionError(message)
        if code in _AUTH_CODES:
            return ProbeConnectionError(
                f"AWS did not accept the recipe's credentials on {action} "
                f"({code}){suffix}; check aws_access_key_id/"
                f"aws_secret_access_key, aws_profile or aws_role"
            )
        if code == "EntityNotFoundException":
            return ValueError(f"{action} found no such object ({code})")
        return ProbeConnectionError(f"{action} failed ({code}){suffix}")
    # Endpoint, proxy, timeout and SSL errors. Named by class only: SSLError's
    # text embeds the transport error verbatim, and a proxy error's the proxy.
    return ProbeConnectionError(f"could not complete {action} ({aws_error_code(exc)})")


@contextmanager
def aws_call(action: str, soft_on_denied: bool = False) -> Iterator[None]:
    """Run AWS SDK calls -- and iterate their paginators, which is where a
    paged call actually fails -- with errors translated. `action` names the
    IAM action and object, e.g. "glue:GetTables on database 'sales'".

    soft_on_denied turns an access denial into a ProbeSoftError; the caller
    must catch it and record the reason (see ProbeSoftError's docstring).
    """
    try:
        yield
    except (ClientError, BotoCoreError) as exc:
        raise _translated(exc, action, soft_on_denied) from exc


def _take(items: Iterable[_T], limit: Optional[int]) -> List[_T]:
    """At most `limit` items, pulling no further page than needed.

    A paginator's search() is lazy, so islice stops the paging itself; on a
    catalog with tens of thousands of tables the discarded pages would be
    real requests.
    """
    return list(items) if limit is None else list(itertools.islice(items, limit))


def _database_record(database: Mapping[str, Any]) -> Dict[str, object]:
    target = database.get("TargetDatabase")
    return {
        "name": database["Name"],
        # "" rather than absent when Glue omits it: ingestion keeps such a
        # database whatever catalog_id says (get_all_databases tests the
        # CatalogId for truthiness), and an absent attribute would make
        # `probe filter --from-run` warn that it cannot tell.
        "catalog_id": database.get("CatalogId") or "",
        # Truthiness, as the JMESPath `[?!TargetDatabase]` in
        # get_all_databases reads it: an empty struct is not a link.
        "resource_link": bool(target),
        "target": (
            f"{target.get('CatalogId', '')}/{target.get('DatabaseName', '')}"
            if target
            else None
        ),
    }


class GlueMetadataProbe:
    """Metadata-only probe over the AWS Glue Data Catalog and Glue jobs.

    Reuses the connector's fetch, never its policy: no getter applies
    database_pattern, table_pattern, ignore_resource_links or the catalog_id
    check, so a name ingestion would drop is reported for `probe filter` to
    explain rather than hidden.
    """

    warnings: List[str]

    def __init__(self, config: GlueSourceConfig) -> None:
        self._config = config
        self._client: Optional["GlueClient"] = None
        self._source: Optional[GlueSource] = None
        self.warnings = []

    @classmethod
    def for_config(cls, config: GlueSourceConfig) -> "GlueMetadataProbe":
        # No client here. Building one resolves the session, which with
        # aws_role calls sts:AssumeRole, and the framework reports a failure
        # raised from for_config with its raw text.
        return cls(config)

    def __enter__(self) -> "GlueMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        # The S3 client is left open: get_s3_client memoizes it on the config.
        if self._client is not None:
            self._client.close()

    @property
    def probe_report(self) -> Optional[GlueSourceReport]:
        """The report ingestion's job-DAG helpers write to (`job_nodes`), so
        their warnings and failures are folded into the result."""
        return self._source.report if self._source is not None else None

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    def _glue(self) -> "GlueClient":
        if self._client is None:
            with aws_call("resolving AWS credentials for Glue"):
                self._client = self._config.get_glue_client()
        return self._client

    def _ingestion_source(self) -> GlueSource:
        if self._source is None:
            glue = self._glue()
            with aws_call("resolving AWS credentials for S3"):
                s3 = self._config.get_s3_client()
            self._source = GlueSource.for_probe(
                self._config, glue_client=glue, s3_client=s3
            )
        return self._source

    def _catalog_label(self) -> str:
        if self._config.catalog_id:
            return f"Glue catalog {self._config.catalog_id}"
        return "the Glue catalog of the recipe's AWS account"

    def _list_databases(self, limit: Optional[int]) -> List[Dict[str, Any]]:
        with aws_call("glue:GetDatabases"):
            pages = (
                self._glue()
                .get_paginator("get_databases")
                .paginate(**glue_catalog_kwargs(self._config.catalog_id))
            )
            return _take(pages.search("DatabaseList"), limit)

    def _database(self, name: str) -> Dict[str, Any]:
        """One database's record, from the listing ingestion reads.

        GetDatabases rather than GetDatabase: ingestion's documented policy
        grants the former only, and the record carries the TargetDatabase and
        CatalogId facts the recipe's non-pattern rules read.
        """
        with aws_call("glue:GetDatabases"):
            pages = (
                self._glue()
                .get_paginator("get_databases")
                .paginate(**glue_catalog_kwargs(self._config.catalog_id))
            )
            found = next(
                (db for db in pages.search("DatabaseList") if db.get("Name") == name),
                None,
            )
        if found is None:
            raise ValueError(
                f"no database named '{name}' in {self._catalog_label()}; "
                f"`probe run databases` lists them"
            )
        return found

    @probe_method(kind=DatasetContainerSubTypes.DATABASE, row_limit_param="limit")
    def databases(self, limit: int = 500) -> List[Dict[str, object]]:
        """Databases in the Glue Data Catalog the recipe reads (the calling
        account's, or catalog_id's when set), in catalog order. Includes
        databases database_pattern, ignore_resource_links or the catalog_id
        check would drop -- a dropped database is reported, not hidden, so
        `probe filter --from-run` can explain it. Each record is name,
        catalog_id (the owning account Glue reports), resource_link (a Lake
        Formation resource link to another catalog's database) and, for a
        link, target as "<account>/<database>". Database parameters and
        descriptions are withheld. Metadata only."""
        return [_database_record(db) for db in self._list_databases(limit)]
