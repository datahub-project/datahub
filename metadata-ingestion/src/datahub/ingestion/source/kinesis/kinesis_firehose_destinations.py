"""Pluggable Firehose destination handlers.

Each handler matches one Firehose destination shape (from boto3
DescribeDeliveryStream) and builds the corresponding DataHub URN. Six handlers
ship in V1; the design supports adding handlers (~50 LOC each) without
touching the orchestrator.
"""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, List, Optional

from datahub.ingestion.source.kinesis.kinesis_config import DestinationPlatform

if TYPE_CHECKING:
    from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport

logger = logging.getLogger(__name__)


class DestinationHandler(ABC):
    """Base class for Firehose destination → DataHub URN handlers.

    Each subclass maps one Firehose destination type (e.g. S3, Redshift) to a
    set of DataHub platform URNs, exposing ``matches()`` and ``build_urns()``.

    Note: not every member of ``DestinationPlatform`` has a subclass here.
    ``"glue"`` is a URN-target platform referenced from SchemaConfiguration on
    Extended S3 destinations (see
    ``ExtendedS3Destination.extract_schema_config_glue_urn``) — it's a URN
    target, not a Firehose destination type, so it has no handler.
    """

    #: Destination platform name in DataHub. Constrained to the closed set
    #: declared by ``DestinationPlatform`` in kinesis_config so
    #: ``destination_platform_map`` keys typecheck end-to-end. The Literal
    #: governs platform values, but not every member has a handler subclass
    #: (see class docstring re ``"glue"``).
    platform: ClassVar[DestinationPlatform]

    @abstractmethod
    def matches(self, destination_description: Dict[str, Any]) -> bool:
        """Return True if this handler can build URN(s) from this destination block."""

    @abstractmethod
    def build_urns(
        self,
        destination_description: Dict[str, Any],
        urn_builder: Callable[..., str],
    ) -> List[str]:
        """Build the DataHub URNs for the destination, using urn_builder to apply
        platform_instance and env overrides from destination_platform_map.

        Returns:
            A list of URNs. Empty list means the destination produced no URNs
            (parse error, missing required fields, etc.) — the orchestrator
            treats this as "no output for this destination". Most handlers
            return a single-element list; only Iceberg, which supports
            multi-table writes, returns more than one.
        """


def _build_s3_urn(
    s3_description: Dict[str, Any], urn_builder: Callable[..., str]
) -> List[str]:
    """Compute the S3 dataset URN from a Firehose S3 destination block.

    Handles both `S3DestinationDescription` (legacy) and
    `ExtendedS3DestinationDescription` (modern) since both shapes share the
    same `BucketARN` + `Prefix` keys. Returns ``[]`` when BucketARN is
    missing/empty — defensive against the silent-malformed-URN pattern
    `urn:li:dataset:(s3,,PROD)`.
    """
    arn = s3_description.get("BucketARN", "")
    bucket = arn.split(":::")[1] if ":::" in arn else arn
    if not bucket:
        return []
    prefix = s3_description.get("Prefix", "")
    name = f"{bucket}/{prefix}" if prefix else bucket
    return [urn_builder(platform="s3", name=name)]


class S3Destination(DestinationHandler):
    """Legacy `S3DestinationDescription` handler. Modern Firehose returns
    `ExtendedS3DestinationDescription` (with the SchemaConfiguration / Glue
    lineage capability) — that is handled by `ExtendedS3Destination`.

    `matches()` explicitly rejects responses that ALSO carry the Extended
    description key so the two handlers are mutually exclusive on any single
    destination dict, making the order in `DESTINATION_HANDLERS` an
    optimization rather than a correctness requirement.
    """

    platform: ClassVar[DestinationPlatform] = "s3"

    def matches(self, d: Dict[str, Any]) -> bool:
        # Reject responses that also carry the Extended key so order in
        # DESTINATION_HANDLERS isn't load-bearing.
        if d.get("ExtendedS3DestinationDescription") is not None:
            return False
        return (
            "S3DestinationDescription" in d
            and d.get("S3DestinationDescription") is not None
        )

    def build_urns(
        self, d: Dict[str, Any], urn_builder: Callable[..., str]
    ) -> List[str]:
        return _build_s3_urn(d["S3DestinationDescription"], urn_builder)


class ExtendedS3Destination(DestinationHandler):
    """Modern Firehose S3 destinations return `ExtendedS3DestinationDescription`
    (basic `S3DestinationDescription` is legacy). Same S3 URN shape (bucket +
    prefix); additionally exposes extract_schema_config_glue_urn for Firehose's
    Parquet/ORC format-conversion path, which references a Glue table.
    """

    platform: ClassVar[DestinationPlatform] = "s3"

    def matches(self, d: Dict[str, Any]) -> bool:
        return (
            "ExtendedS3DestinationDescription" in d
            and d.get("ExtendedS3DestinationDescription") is not None
        )

    def build_urns(
        self, d: Dict[str, Any], urn_builder: Callable[..., str]
    ) -> List[str]:
        return _build_s3_urn(d["ExtendedS3DestinationDescription"], urn_builder)

    def extract_schema_config_glue_urn(
        self,
        d: Dict[str, Any],
        urn_builder: Callable[..., str],
        report: "KinesisSourceReport",
        delivery_stream_name: str,
    ) -> Optional[str]:
        """Resolve the Glue table reference (if any) on this Extended S3
        destination's format-conversion config.

        Returns the Glue URN to add as upstream lineage, or ``None`` for both
        "no SchemaConfiguration" (silent) and "SchemaConfiguration present but
        malformed" (in which case the skip is recorded on ``report``
        directly). Two outcomes share the ``None`` return because the caller
        does the same thing in both cases (skip the lineage edge); the
        difference is purely in whether the user should be told via the
        report. We do that here so the caller's code path stays simple.

        Required fields: ``DatabaseName`` and ``TableName`` only. CatalogId is
        *not* required — AWS omits it from ``DescribeDeliveryStream`` responses
        when it equals the caller's account (per AWS docs, CatalogId is an
        input-side default). DataHub's Glue dataset URN format
        ``urn:li:dataset:(glue, <db>.<table>, <env>)`` doesn't include CatalogId
        anyway, so its absence on the AWS side has no effect on URN correctness.
        Cross-account Glue catalogs would still resolve to the same URN —
        users who need cross-account disambiguation set
        ``destination_platform_map.glue.platform_instance``.
        """
        s3 = d["ExtendedS3DestinationDescription"]
        fmt = s3.get("DataFormatConversionConfiguration") or {}
        schema_cfg = fmt.get("SchemaConfiguration") or {}
        if not schema_cfg:
            return None  # absent — Firehose isn't doing format conversion. Silent.
        missing = [f for f in ("DatabaseName", "TableName") if not schema_cfg.get(f)]
        if missing:
            report.report_firehose_glue_schema_skipped(
                delivery_stream_name,
                f"SchemaConfiguration missing {', '.join(missing)}",
            )
            return None
        name = f"{schema_cfg['DatabaseName']}.{schema_cfg['TableName']}"
        return urn_builder(platform="glue", name=name)


class RedshiftDestination(DestinationHandler):
    platform: ClassVar[DestinationPlatform] = "redshift"

    def matches(self, d: Dict[str, Any]) -> bool:
        return (
            "RedshiftDestinationDescription" in d
            and d.get("RedshiftDestinationDescription") is not None
        )

    def build_urns(
        self, d: Dict[str, Any], urn_builder: Callable[..., str]
    ) -> List[str]:
        rs = d["RedshiftDestinationDescription"]
        copy_cmd = rs.get("CopyCommand", {})
        table = copy_cmd.get("DataTableName", "")
        # Firehose returns "<schema>.<table>" in CopyCommand.DataTableName.
        # The full DataHub Redshift URN uses "<db>.<schema>.<table>" — parse db from JDBC.
        jdbc = rs.get("ClusterJDBCURL", "")
        db = _parse_redshift_db(jdbc)
        # Refuse to emit a malformed URN when DB or schema.table cannot be
        # parsed. The old fallback (`name = table` when db is None) produced
        # `urn:li:dataset:(redshift,my_table,PROD)` — a syntactically valid
        # but semantically wrong URN missing the database+schema prefix that
        # would never resolve in DataHub. Matches the SnowflakeDestination
        # `if not all([db, schema, table])` discipline.
        if not db or "." not in table:
            return []
        name = f"{db}.{table}"
        return [urn_builder(platform="redshift", name=name)]


def _parse_redshift_db(jdbc_url: str) -> Optional[str]:
    """Extract the database name from a Redshift JDBC URL.

    Format: jdbc:redshift://<host>:<port>/<db>?options

    Returns None when the URL is malformed (no path component, host fragment
    only, etc.). The old permissive split would accept `jdbc:redshift://host:5439`
    and return "host:5439" as the "database" — building a URN that referenced
    a host:port as if it were a Redshift catalog. We now require a real path.
    """
    if not jdbc_url or "://" not in jdbc_url:
        return None
    # Strip the scheme so split-on-/ doesn't pick up the `://`.
    after_scheme = jdbc_url.split("://", 1)[1]
    if "/" not in after_scheme:
        return None  # host[:port] with no path
    tail = after_scheme.rsplit("/", 1)[-1]
    db = tail.split("?")[0] if tail else None
    if not db:
        return None
    # Reject host:port-like fragments — real database names don't contain colons.
    if ":" in db:
        return None
    return db


class OpenSearchDestination(DestinationHandler):
    """OpenSearch / Elasticsearch destination — DataHub uses 'elasticsearch' for both."""

    platform: ClassVar[DestinationPlatform] = "elasticsearch"

    def matches(self, d: Dict[str, Any]) -> bool:
        # Boto3 uses 'AmazonopensearchserviceDestinationDescription' (the AWS public API
        # name predates the OpenSearch rebrand for older accounts).
        keys = (
            "AmazonopensearchserviceDestinationDescription",
            "ElasticsearchDestinationDescription",
        )
        return any(k in d and d.get(k) is not None for k in keys)

    def build_urns(
        self, d: Dict[str, Any], urn_builder: Callable[..., str]
    ) -> List[str]:
        for k in (
            "AmazonopensearchserviceDestinationDescription",
            "ElasticsearchDestinationDescription",
        ):
            block = d.get(k)
            if block is not None:
                index = block.get("IndexName")
                if index:
                    return [urn_builder(platform="elasticsearch", name=index)]
        return []


class SnowflakeDestination(DestinationHandler):
    """Snowflake destination.

    URN case-folding is applied centrally by the orchestrator's
    ``_destination_urn``, controlled by
    ``destination_platform_map.snowflake.convert_urns_to_lowercase``
    (default ``True`` to match the Snowflake DataHub source's default).
    """

    platform: ClassVar[DestinationPlatform] = "snowflake"

    def matches(self, d: Dict[str, Any]) -> bool:
        return (
            "SnowflakeDestinationDescription" in d
            and d.get("SnowflakeDestinationDescription") is not None
        )

    def build_urns(
        self, d: Dict[str, Any], urn_builder: Callable[..., str]
    ) -> List[str]:
        sd = d["SnowflakeDestinationDescription"]
        db = sd.get("Database", "")
        schema = sd.get("Schema", "")
        table = sd.get("Table", "")
        if not all([db, schema, table]):
            return []
        name = f"{db}.{schema}.{table}"
        return [urn_builder(platform="snowflake", name=name)]


class IcebergDestination(DestinationHandler):
    """Apache Iceberg destination — URN names use <namespace>.<table> format.

    Iceberg DataHub source supports REST catalog and Hive metastore conventions;
    REST catalog uses dot-separated namespaces. V1 assumes REST format.

    Firehose's Iceberg destination is the only one (of V1's seven supported
    types) that targets multiple downstream tables from a single delivery
    stream — `DestinationTableConfigurationList` is a list. This handler
    emits one lineage URN per configured table.
    """

    platform: ClassVar[DestinationPlatform] = "iceberg"

    def matches(self, d: Dict[str, Any]) -> bool:
        return (
            "IcebergDestinationDescription" in d
            and d.get("IcebergDestinationDescription") is not None
        )

    def build_urns(
        self, d: Dict[str, Any], urn_builder: Callable[..., str]
    ) -> List[str]:
        ice = d["IcebergDestinationDescription"]
        tables: List[Dict[str, Any]] = (
            ice.get("DestinationTableConfigurationList") or []
        )
        urns: List[str] = []
        for t in tables:
            db = t.get("DestinationDatabaseName", "")
            name = t.get("DestinationTableName", "")
            if not name:
                # Skip malformed table entries silently — they don't represent
                # a real lineage edge. Other valid entries in the list still
                # produce their URNs.
                continue
            full = f"{db}.{name}" if db else name
            urns.append(urn_builder(platform="iceberg", name=full))
        return urns


class MongoDBDestination(DestinationHandler):
    """MongoDB / DocumentDB destination — URN name is <database>.<collection>."""

    platform: ClassVar[DestinationPlatform] = "mongodb"

    def matches(self, d: Dict[str, Any]) -> bool:
        return (
            "MongoDBDestinationDescription" in d
            and d.get("MongoDBDestinationDescription") is not None
        )

    def build_urns(
        self, d: Dict[str, Any], urn_builder: Callable[..., str]
    ) -> List[str]:
        m = d["MongoDBDestinationDescription"]
        db = m.get("DatabaseName", "")
        collection = m.get("CollectionName", "")
        if not all([db, collection]):
            return []
        return [urn_builder(platform="mongodb", name=f"{db}.{collection}")]


# Registry order doesn't matter for matching (each handler short-circuits via
# `matches()` rejecting the other handlers' description keys), but is preserved
# for predictable iteration during debugging.
DESTINATION_HANDLERS: List[DestinationHandler] = [
    ExtendedS3Destination(),  # match modern S3 first
    S3Destination(),
    RedshiftDestination(),
    OpenSearchDestination(),
    SnowflakeDestination(),
    IcebergDestination(),
    MongoDBDestination(),
]
