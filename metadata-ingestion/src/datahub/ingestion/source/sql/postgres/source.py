import logging
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

# This import verifies that the dependencies are available.
import psycopg2  # noqa: F401
import sqlalchemy.dialects.postgresql as custom_types

# GeoAlchemy adds support for PostGIS extensions in SQLAlchemy. Importing it
# hooks PostGIS reflection into SQLAlchemy, and the imported types are also
# registered in the DataHub type mapping below. For more details, see here:
# https://geoalchemy-2.readthedocs.io/en/latest/core_tutorial.html#reflecting-tables.
from geoalchemy2 import Geography, Geometry, Raster
from pydantic import BaseModel, field_validator, model_validator
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import UserDefinedType

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

from typing_extensions import Annotated

from datahub.configuration.common import AllowDenyPattern, Filters
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.agent.sql_gate import (
    INFORMATION_SCHEMA,
    CatalogScope,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    RDSIAMTokenManager,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.sql.postgres.lineage import PostgresLineageExtractor
from datahub.ingestion.source.sql.postgres.query import (
    POSTGRES_SYSTEM_DATABASES,
    PostgresQuery,
)
from datahub.ingestion.source.sql.rds_iam import RDSIAMConnectionMixin
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.sql.stored_procedures.models import (
    BaseProcedure,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BytesTypeClass,
    MapTypeClass,
    StringTypeClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.str_enum import StrEnum

logger: logging.Logger = logging.getLogger(__name__)

register_custom_type(custom_types.ARRAY, ArrayTypeClass)
register_custom_type(custom_types.JSON, BytesTypeClass)
register_custom_type(custom_types.JSONB, BytesTypeClass)
register_custom_type(custom_types.HSTORE, MapTypeClass)


class _PostgresCustomType(UserDefinedType):
    """Placeholder for postgres types that SQLAlchemy does not ship.

    Registering these in ``ischema_names`` keeps their columns from reflecting
    as ``NullType``, which would both classify them as DataHub NullType and
    replace their native type name with the literal string "null".

    ``UserDefinedType`` with ``get_col_spec`` is used instead of the existing
    ``make_sqlalchemy_type`` helper because that helper sets
    ``impl = LargeBinary``, which would compile these types to a misleading
    ``BYTEA`` native type name; this route preserves the real type name,
    including its modifier (e.g. ``VECTOR(4)``).
    """

    # No default on purpose: a subclass that forgets to set it fails loudly at
    # first compile instead of silently emitting an empty nativeDataType.
    type_name: ClassVar[str]

    def __init__(self, dimensions: Optional[int] = None) -> None:
        # Reflection passes the type modifier through as a single int
        # (e.g. vector(4) -> dimensions=4). A named parameter — rather than
        # *args — is required for SQLAlchemy's statement-cache key to include
        # the modifier (VECTOR(4) vs VECTOR(1536) must not share a key), and
        # it also survives adapt()/constructor_copy().
        self.dimensions = dimensions

    def get_col_spec(self, **kw: Any) -> str:
        if self.dimensions is not None:
            return f"{self.type_name}({self.dimensions})"
        return self.type_name


def _make_postgres_type(name: str) -> Type[_PostgresCustomType]:
    assert name, "postgres placeholder types need a non-empty type name"
    # cache_ok must be in each subclass's own __dict__ — SQLAlchemy does not
    # consult the MRO for it, and without it every statement touching one of
    # these columns is uncacheable and emits an SAWarning.
    postgres_type: Type[_PostgresCustomType] = type(
        name, (_PostgresCustomType,), {"type_name": name, "cache_ok": True}
    )
    return postgres_type


# pgvector (https://github.com/pgvector/pgvector)
VECTOR = _make_postgres_type("VECTOR")
HALFVEC = _make_postgres_type("HALFVEC")
SPARSEVEC = _make_postgres_type("SPARSEVEC")
# Built-in geometric types (https://www.postgresql.org/docs/current/datatype-geometric.html)
POINT = _make_postgres_type("POINT")
LINE = _make_postgres_type("LINE")
LSEG = _make_postgres_type("LSEG")
BOX = _make_postgres_type("BOX")
PATH = _make_postgres_type("PATH")
POLYGON = _make_postgres_type("POLYGON")
CIRCLE = _make_postgres_type("CIRCLE")
XML = _make_postgres_type("XML")
LTREE = _make_postgres_type("LTREE")
CITEXT = _make_postgres_type("CITEXT")
# PG14+ multirange counterparts of the range types; SQLAlchemy only ships
# these natively from 2.0, so under the current 1.4 pin they need placeholders
# too (the setdefault below yields to the native types after an upgrade).
INT4MULTIRANGE = _make_postgres_type("INT4MULTIRANGE")
INT8MULTIRANGE = _make_postgres_type("INT8MULTIRANGE")
NUMMULTIRANGE = _make_postgres_type("NUMMULTIRANGE")
DATEMULTIRANGE = _make_postgres_type("DATEMULTIRANGE")
TSMULTIRANGE = _make_postgres_type("TSMULTIRANGE")
TSTZMULTIRANGE = _make_postgres_type("TSTZMULTIRANGE")

# PostGIS types are reflected via the geoalchemy2 import above; map them so
# their columns stop falling back to NullType. BytesTypeClass (not
# RecordTypeClass, which signals a struct with nested sub-fields) follows the
# Teradata/Snowflake precedent for opaque geospatial scalars — PostGIS values
# really are WKB on the wire.
register_custom_type(Geometry, BytesTypeClass)
register_custom_type(Geography, BytesTypeClass)
register_custom_type(Raster, BytesTypeClass)

for _vector_type in (VECTOR, HALFVEC, SPARSEVEC):
    register_custom_type(_vector_type, ArrayTypeClass)
for _geometric_type in (POINT, LINE, LSEG, BOX, PATH, POLYGON, CIRCLE):
    register_custom_type(_geometric_type, BytesTypeClass)
for _string_like_type in (XML, LTREE, CITEXT):
    register_custom_type(_string_like_type, StringTypeClass)

register_custom_type(custom_types.CIDR, StringTypeClass)
for _range_type in (
    custom_types.INT4RANGE,
    custom_types.INT8RANGE,
    custom_types.NUMRANGE,
    custom_types.DATERANGE,
    custom_types.TSRANGE,
    custom_types.TSTZRANGE,
):
    register_custom_type(_range_type, StringTypeClass)
for _multirange_type in (
    INT4MULTIRANGE,
    INT8MULTIRANGE,
    NUMMULTIRANGE,
    DATEMULTIRANGE,
    TSMULTIRANGE,
    TSTZMULTIRANGE,
):
    register_custom_type(_multirange_type, StringTypeClass)

# If the pgvector SQLAlchemy integration is installed, importing it registers
# a full-featured `vector` type in ischema_names (it parses dimensions
# properly). Prefer it over the placeholder — the setdefault below yields to
# it — and map it to the same DataHub type.
try:
    from pgvector.sqlalchemy import Vector as _PgVectorType

    register_custom_type(_PgVectorType, ArrayTypeClass)
except ImportError:
    pass

# ischema_names is process-global state shared by every PGDialect subclass in
# the process (CockroachDB, TimescaleDB, ... inherit these entries; Redshift
# does not go through this source). setdefault instead of update so a real
# type implementation registered by another library (pgvector above,
# SQLAlchemy 2.0.7+'s own CITEXT, ...) is never clobbered by a placeholder.
#
# Reflection precedence caveat: PGDialect._get_column_info consults
# ischema_names *before* user-defined domains, so a domain named exactly like
# one of these entries (e.g. "xml", "box") now resolves to the placeholder and
# skips the domain branch — including its nullability and default handling.
for _type_name, _placeholder_type in {
    "vector": VECTOR,
    "halfvec": HALFVEC,
    "sparsevec": SPARSEVEC,
    "point": POINT,
    "line": LINE,
    "lseg": LSEG,
    "box": BOX,
    "path": PATH,
    "polygon": POLYGON,
    "circle": CIRCLE,
    "xml": XML,
    "ltree": LTREE,
    "citext": CITEXT,
    "int4multirange": INT4MULTIRANGE,
    "int8multirange": INT8MULTIRANGE,
    "nummultirange": NUMMULTIRANGE,
    "datemultirange": DATEMULTIRANGE,
    "tsmultirange": TSMULTIRANGE,
    "tstzmultirange": TSTZMULTIRANGE,
}.items():
    custom_types.base.ischema_names.setdefault(_type_name, _placeholder_type)


VIEW_LINEAGE_QUERY = """
WITH RECURSIVE view_deps AS (
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
WHERE NOT (dependent_ns.nspname = source_ns.nspname AND dependent_view.relname = source_table.relname)
UNION
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
INNER JOIN view_deps vd
    ON vd.dependent_schema = source_ns.nspname
    AND vd.dependent_view = source_table.relname
    AND NOT (dependent_ns.nspname = vd.dependent_schema AND dependent_view.relname = vd.dependent_view)
)


SELECT source_table, source_schema, dependent_view, dependent_schema
FROM view_deps
WHERE NOT (source_schema = 'information_schema' OR source_schema = 'pg_catalog')
ORDER BY source_schema, source_table;
"""


class ViewLineageEntry(BaseModel):
    # note that the order matches our query above
    # so pydantic is able to parse the tuple using parse_obj
    source_table: str
    source_schema: str
    dependent_view: str
    dependent_schema: str


class PostgresAuthMode(StrEnum):
    """Authentication mode for PostgreSQL connection."""

    PASSWORD = "PASSWORD"
    AWS_IAM = "AWS_IAM"


class BasePostgresConfig(RDSIAMConnectionMixin, BasicSQLAlchemyConfig):
    scheme: str = Field(default="postgresql+psycopg2", description="database scheme")
    schema_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.SCHEMA)
    ] = Field(default=AllowDenyPattern(deny=["information_schema"]))

    # Authentication configuration
    auth_mode: PostgresAuthMode = Field(
        default=PostgresAuthMode.PASSWORD,
        description="Authentication mode to use for the PostgreSQL connection. "
        "Options are 'PASSWORD' (default) for standard username/password authentication, "
        "or 'AWS_IAM' for AWS RDS IAM authentication.",
    )
    aws_config: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description="AWS configuration for RDS IAM authentication (only used when auth_mode is AWS_IAM). "
        "Provides full control over AWS credentials, region, profiles, role assumption, retry logic, and proxy settings. "
        "If not explicitly configured, boto3 will automatically use the default credential chain and region from "
        "environment variables (AWS_DEFAULT_REGION, AWS_REGION), AWS config files (~/.aws/config), or IAM role metadata.",
    )

    def rds_iam_enabled(self) -> bool:
        return self.auth_mode == PostgresAuthMode.AWS_IAM

    def rds_iam_default_port(self) -> int:
        return 5432

    def apply_rds_iam_ssl(self, cparams: Dict[str, Any]) -> None:
        # IAM tokens are bearer credentials, so TLS is required rather than
        # preferred. An explicitly stronger mode is left alone.
        if cparams.get("sslmode") not in ("require", "verify-ca", "verify-full"):
            cparams["sslmode"] = "require"

    def probe_prepare_engine(self, engine: Any) -> None:
        # Without this, an AWS_IAM recipe cannot be probed at all: the password
        # is a token injected per connection, so a bare create_engine() has no
        # credential to connect with.
        self.install_rds_iam_auth(engine)

    @classmethod
    def probe_catalog_scope(cls) -> CatalogScope:
        # pg_catalog is named relation by relation, NOT allowed at schema level.
        # It was a schema-level allow with three exclusions, and the comment
        # beside it conceded the risk in as many words -- "the exclusions have to
        # be complete, and nothing tells you when they are not". They were not,
        # and the gap was worse than query text:
        #
        #   pg_stats, pg_statistic  -- most_common_vals and histogram_bounds are
        #     literal sampled values out of user columns. Not a WHERE-clause
        #     literal inside a query string: the row values themselves.
        #   pg_largeobject, pg_largeobject_metadata -- raw bytes of user large
        #     objects.
        #   pg_shadow, pg_authid -- role password hashes.
        #
        # The list below is derived from postgres/query.py and source.py (what
        # ingestion reads) plus the structural counterparts an agent reaches for,
        # the same way the Redshift and MSSQL declarations are built.
        # Inherited by CockroachDB and TimescaleDB.
        #
        # Deliberately absent, and why:
        #   pg_stat_statements, pg_stat_activity, pg_prepared_statements --
        #     statement text.
        #   the pg_stats/pg_largeobject/pg_shadow families above.
        #   pg_user, pg_roles, pg_authid, pg_auth_members, pg_user_mapping --
        #     user identity rather than schema shape. This matches Redshift,
        #     which withholds pg_user/svv_user_info, and Snowflake, which
        #     withholds account_usage.users.
        return CatalogScope(
            schemas=frozenset({INFORMATION_SCHEMA}),
            relations=frozenset(
                {
                    # Core catalog: names, columns, types, defaults, comments.
                    "pg_catalog.pg_class",
                    "pg_catalog.pg_namespace",
                    "pg_catalog.pg_database",
                    "pg_catalog.pg_attribute",
                    "pg_catalog.pg_attrdef",
                    "pg_catalog.pg_type",
                    "pg_catalog.pg_description",
                    "pg_catalog.pg_index",
                    "pg_catalog.pg_constraint",
                    "pg_catalog.pg_inherits",
                    "pg_catalog.pg_sequence",
                    "pg_catalog.pg_enum",
                    # Read by ingestion for lineage and stored procedures.
                    "pg_catalog.pg_depend",
                    "pg_catalog.pg_rewrite",
                    "pg_catalog.pg_proc",
                    "pg_catalog.pg_language",
                    "pg_catalog.pg_extension",
                    # The friendly views over the above. Their *_def columns are
                    # object DDL, which is schema and which ingestion publishes
                    # as dataset properties -- consistent with permitting
                    # Snowflake's ACCOUNT_USAGE.VIEWS.
                    "pg_catalog.pg_tables",
                    "pg_catalog.pg_views",
                    "pg_catalog.pg_matviews",
                    "pg_catalog.pg_indexes",
                }
            ),
        )


class PostgresConfig(BasePostgresConfig, BaseUsageConfig):
    database_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.DATABASE)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for databases to filter in ingestion. "
            "Note: this is not used if `database` or `sqlalchemy_uri` are provided."
        ),
    )
    database: Optional[str] = Field(
        default=None,
        description="database (catalog). If set to Null, all databases will be considered for ingestion.",
    )
    initial_database: str = Field(
        default="postgres",
        description=(
            "Initial database used to query for the list of databases, when ingesting multiple databases. "
            "Note: this is not used if `database` or `sqlalchemy_uri` are provided."
        ),
    )

    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures.",
    )

    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion."
        "Specify regex to match the entire procedure name in database.schema.procedure_name format. e.g. to match all procedures starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )

    include_query_lineage: bool = Field(
        default=False,
        description=(
            "Enable query-based lineage extraction from pg_stat_statements. "
            "Requires the pg_stat_statements extension to be installed and enabled. "
            "See documentation for setup instructions."
        ),
    )

    max_queries_to_extract: int = Field(
        default=1000,
        description=(
            "Maximum number of queries to extract from pg_stat_statements "
            "for lineage analysis. Queries are prioritized by execution time and frequency."
        ),
    )

    min_query_calls: int = Field(
        default=1,
        description=(
            "Minimum number of executions required for a query to be included. "
            "Set higher to focus on frequently-used queries."
        ),
    )

    query_exclude_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "SQL LIKE patterns to exclude from query extraction. "
            "Example: ['%pg_catalog%', '%temp_%'] to exclude catalog and temp tables."
        ),
    )

    include_usage_statistics: bool = Field(
        default=False,
        description=(
            "Generate usage statistics from query history. Requires include_query_lineage to be enabled. "
            "Collects metrics like unique user counts, query frequencies, and column access patterns. "
            "Statistics appear in DataHub UI under the Dataset Profile > Usage tab."
        ),
    )

    @field_validator("max_queries_to_extract")
    @classmethod
    def validate_max_queries_to_extract(cls, value: int) -> int:
        """Validate max_queries_to_extract is within reasonable range."""
        if value <= 0:
            raise ValueError(
                "max_queries_to_extract must be positive. "
                "Please set it to a value >= 1 (e.g., 1000)."
            )
        if value > 10000:
            raise ValueError(
                "max_queries_to_extract must be <= 10000 to avoid memory issues. "
                "Please reduce the value to 10000 or less."
            )
        return value

    @field_validator("min_query_calls")
    @classmethod
    def validate_min_query_calls(cls, value: int) -> int:
        """Validate min_query_calls is non-negative."""
        if value < 0:
            raise ValueError(
                "min_query_calls must be non-negative. "
                "Please set it to 0 or a positive integer (e.g., 1)."
            )
        return value

    @field_validator("query_exclude_patterns")
    @classmethod
    def validate_query_exclude_patterns(
        cls, value: Optional[List[str]]
    ) -> Optional[List[str]]:
        """Validate query_exclude_patterns has reasonable limits."""
        if value is None:
            return value

        if len(value) > 100:
            raise ValueError(
                "query_exclude_patterns must have <= 100 patterns to avoid performance issues. "
                f"Please reduce from {len(value)} to 100 or fewer patterns."
            )

        for pattern in value:
            if len(pattern) > 500:
                raise ValueError(
                    f"Pattern '{pattern[:50]}...' exceeds 500 characters (length: {len(pattern)}). "
                    "Use shorter patterns to avoid performance issues. "
                    "Please simplify your pattern or split it into multiple shorter patterns."
                )

        return value

    @model_validator(mode="after")
    def validate_usage_statistics_dependency(self) -> "PostgresConfig":
        """Validate that include_usage_statistics requires include_query_lineage."""
        if self.include_usage_statistics and not self.include_query_lineage:
            raise ValueError(
                "include_usage_statistics requires include_query_lineage to be enabled. "
                "Please add 'include_query_lineage: true' to your configuration."
            )
        return self

    # --- Agent probe contract (see datahub.ingestion.agent.probe_methods) ---
    def list_databases(self, conn: Connection) -> List[str]:
        # Raw database listing shared with get_inspectors() below -- no
        # database_pattern applied here; callers (get_inspectors() and the
        # Database-level agent probe below) apply that themselves, so the two
        # paths query the exact same rows instead of each re-deriving the
        # listing SQL.
        return PostgresQuery.list_databases(conn)

    @classmethod
    def default_databases(cls) -> FrozenSet[str]:
        # Databases this source drops regardless of database_pattern -- Postgres
        # template databases and AWS RDS's internal admin database. Same shape
        # as SQLCommonConfig.default_schemas() one level down: lets the
        # Database-level probe below report one of these as
        # excluded_by: "default_database" instead of it silently never
        # appearing. Reuses PostgresQuery's own exclusion list so the probe
        # and the query it mirrors cannot drift apart.
        return frozenset(POSTGRES_SYSTEM_DATABASES)


@platform_name("Postgres")
@config_class(PostgresConfig)
@support_status(SupportStatus.GA)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class PostgresSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, tables, and stored procedures
    - Column types associated with each table
    - Also supports PostGIS extensions
    - Table, row, and column statistics via optional SQL profiling
    """

    config: PostgresConfig

    def __init__(self, config: PostgresConfig, ctx: PipelineContext):
        super().__init__(config, ctx, self.get_platform())

        # Built by the config, not here, so `datahub recipe probe` gets the same
        # token manager off the same object -- see RDSIAMConnectionMixin. Called
        # eagerly so a recipe that asks for IAM without a port or username still
        # fails at construction, as it did when this block lived here.
        self._rds_iam_token_manager: Optional[RDSIAMTokenManager] = (
            config.rds_iam_token_manager()
        )

        self.sql_aggregator: Optional[SqlParsingAggregator] = None
        if self.config.include_query_lineage:
            # Validate graph connection requirement for usage statistics
            if self.config.include_usage_statistics and self.ctx.graph is None:
                error_message = (
                    "Usage statistics generation requires a DataHub graph connection (ctx.graph). "
                    "You have enabled 'include_usage_statistics: true' but no graph connection is available. "
                    "Please provide a graph connection in your pipeline configuration or disable usage statistics."
                )
                logger.error(error_message)
                self.report.failure(
                    message=error_message,
                    context="usage_statistics_graph_validation_failed",
                )
                raise ValueError(error_message)

            try:
                self.sql_aggregator = SqlParsingAggregator(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                    graph=self.ctx.graph,
                    generate_lineage=True,
                    generate_queries=True,
                    generate_usage_statistics=self.config.include_usage_statistics,
                    usage_config=self.config
                    if self.config.include_usage_statistics
                    else None,
                )
                logger.info(
                    "SQL parsing aggregator initialized for query-based lineage"
                )
            except Exception as e:
                error_message = (
                    f"Failed to initialize SQL parsing aggregator for query-based lineage: {e}. "
                    f"You have explicitly enabled 'include_query_lineage: true' in your configuration. "
                    f"Common causes: missing DataHub graph connection, insufficient permissions, "
                    f"or missing dependencies. Please check your configuration and try again."
                )
                logger.error(error_message)
                self.report.failure(
                    message=error_message,
                    context="sql_aggregator_init_failed",
                )
                raise RuntimeError(error_message) from e

    def get_platform(self) -> str:
        return "postgres"

    @classmethod
    def create(cls, config_dict, ctx):
        config = PostgresConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _setup_rds_iam_event_listener(
        self, engine: "Engine", database_name: Optional[str] = None
    ) -> None:
        """Inject RDS IAM tokens on this engine's connections.

        One line, because the implementation is on the config: the probe builds
        its own engines and can only reach setup that lives there. `database_name`
        is unused and kept for the call sites -- the token is per host, not per
        database.
        """
        self.config.install_rds_iam_auth(engine)

    def get_inspectors(self) -> Iterable[Inspector]:
        # Note: get_sql_alchemy_url will choose `sqlalchemy_uri` over the passed in database
        url = self.config.get_sql_alchemy_url(
            database=self.config.database or self.config.initial_database
        )

        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)
        self._setup_rds_iam_event_listener(engine)

        with engine.connect() as conn:
            if self.config.database or self.config.sqlalchemy_uri:
                inspector = inspect(conn)
                yield inspector
            else:
                databases = self.config.list_databases(conn)
                for db_name in databases:
                    if not self.config.database_pattern.allowed(db_name):
                        continue

                    url = self.config.get_sql_alchemy_url(database=db_name)
                    db_engine = create_engine(url, **self.config.options)
                    self._setup_rds_iam_event_listener(db_engine, database_name=db_name)

                    with db_engine.connect() as conn:
                        inspector = inspect(conn)
                        yield inspector

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_workunits_internal()

        if self.views_failed_parsing:
            for inspector in self.get_inspectors():
                if self.config.include_view_lineage:
                    yield from self._get_view_lineage_workunits(inspector)

        if self.config.include_query_lineage and self.sql_aggregator:
            yield from self._get_query_based_lineage_workunits()

    def _get_view_lineage_elements(
        self, inspector: Inspector
    ) -> Dict[Tuple[str, str], List[str]]:
        data: List[ViewLineageEntry] = []
        with inspector.engine.connect() as conn:
            results = conn.execute(VIEW_LINEAGE_QUERY)
            if results.returns_rows is False:
                return {}

            for row in results:
                data.append(ViewLineageEntry.model_validate(row))

        lineage_elements: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        # Loop over the lineages in the JSON data.
        for lineage in data:
            if not self.config.view_pattern.allowed(lineage.dependent_view):
                self.report.report_dropped(
                    f"{lineage.dependent_schema}.{lineage.dependent_view}"
                )
                continue

            if not self.config.schema_pattern.allowed(lineage.dependent_schema):
                self.report.report_dropped(
                    f"{lineage.dependent_schema}.{lineage.dependent_view}"
                )
                continue

            key = (lineage.dependent_view, lineage.dependent_schema)
            # Append the source table to the list.
            lineage_elements[key].append(
                mce_builder.make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=self.get_identifier(
                        schema=lineage.source_schema,
                        entity=lineage.source_table,
                        inspector=inspector,
                    ),
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )
            )

        return lineage_elements

    def _get_view_lineage_workunits(
        self, inspector: Inspector
    ) -> Iterable[MetadataWorkUnit]:
        lineage_elements = self._get_view_lineage_elements(inspector)

        if not lineage_elements:
            return

        for key, source_tables in lineage_elements.items():
            dependent_view, dependent_schema = key

            # Construct a lineage object.
            view_identifier = self.get_identifier(
                schema=dependent_schema, entity=dependent_view, inspector=inspector
            )
            if view_identifier not in self.views_failed_parsing:
                continue
            urn = mce_builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=view_identifier,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            # use the mce_builder to ensure that the change proposal inherits
            # the correct defaults for auditHeader and systemMetadata
            lineage_mce = mce_builder.make_lineage_mce(
                source_tables,
                urn,
            )

            for item in mcps_from_mce(lineage_mce):
                yield item.as_workunit()

    def _get_query_based_lineage_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract and emit query-based lineage using pg_stat_statements.

        This supplements view-based lineage with lineage extracted from
        executed queries (INSERT INTO SELECT, CTAS, etc.).
        """
        logger.info("Starting query-based lineage extraction from pg_stat_statements")

        for inspector in self.get_inspectors():
            if self.sql_aggregator is None:
                logger.warning(
                    "SQL aggregator not initialized, skipping query-based lineage extraction. "
                    "Check initialization errors above."
                )
                self.report.warning(
                    message=(
                        "Query-based lineage was enabled but SQL aggregator failed to initialize. "
                        "No query-based lineage will be extracted. Check earlier error messages."
                    ),
                    context="query_lineage_skipped",
                    log=False,
                )
                return

            with inspector.engine.connect() as connection:
                lineage_extractor = PostgresLineageExtractor(
                    config=self.config,
                    connection=connection,
                    report=self.report,
                    sql_aggregator=self.sql_aggregator,
                    default_schema="public",
                )

                try:
                    lineage_extractor.populate_lineage_from_queries()
                except Exception as e:
                    logger.error(
                        "Failed to populate lineage from queries: %s. "
                        "Continuing with other lineage sources.",
                        e,
                    )
                    self.report.failure(
                        message=(
                            "Query lineage extraction failed. "
                            "Check that pg_stat_statements extension is properly configured and accessible. "
                            "See documentation for setup instructions: "
                            "https://datahubproject.io/docs/generated/ingestion/sources/postgres"
                        ),
                        context="query_lineage_extraction_failed",
                        exc=e,
                    )

        with PerfTimer() as timer:
            mcp_count = 0
            if self.sql_aggregator:
                try:
                    mcp: MetadataChangeProposalWrapper
                    for mcp in self.sql_aggregator.gen_metadata():
                        yield mcp.as_workunit()
                        mcp_count += 1
                except Exception as e:
                    logger.error(
                        "Failed to generate metadata from SQL aggregator: %s",
                        e,
                    )
                    self.report.failure(
                        message=(
                            "Lineage metadata generation failed. "
                            "This may indicate issues with the DataHub graph connection or schema resolution. "
                            "Check your graph configuration and ensure all required schemas are accessible."
                        ),
                        context="lineage_metadata_generation_failed",
                        exc=e,
                    )

        logger.info(
            f"Generated {mcp_count} lineage workunits from queries "
            f"in {timer.elapsed_seconds():.2f} seconds"
        )

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        regular = f"{schema}.{entity}"
        if self.config.database:
            return f"{self.config.database}.{regular}"
        current_database = self.get_db_name(inspector)
        return f"{current_database}.{regular}"

    def add_profile_metadata(self, inspector: Inspector) -> None:
        try:
            with inspector.engine.connect() as conn:
                for row in conn.execute(
                    """SELECT table_catalog, table_schema, table_name, pg_table_size('"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"') AS table_size FROM information_schema.TABLES"""
                ):
                    self.profile_metadata_info.dataset_name_to_storage_bytes[
                        self.get_identifier(
                            schema=row.table_schema,
                            entity=row.table_name,
                            inspector=inspector,
                        )
                    ] = row.table_size
        except Exception as e:
            logger.error(
                f"Failed to fetch profile metadata: {e}. "
                f"This may indicate insufficient permissions to query information_schema.TABLES or use pg_table_size(). "
                f"Profiling will continue without storage size information."
            )

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """
        Get stored procedures for a specific schema.
        """
        base_procedures = []
        with inspector.engine.connect() as conn:
            procedures = conn.execute(
                """
                    SELECT
                        p.proname AS name,
                        l.lanname AS language,
                        pg_get_function_arguments(p.oid) AS arguments,
                        p.prosrc AS definition,
                        obj_description(p.oid, 'pg_proc') AS comment
                    FROM
                        pg_proc p
                    JOIN
                        pg_namespace n ON n.oid = p.pronamespace
                    JOIN
                        pg_language l ON l.oid = p.prolang
                    WHERE
                        p.prokind = 'p'
                        AND n.nspname = %s;
                """,
                (schema,),
            )

            procedure_rows = list(procedures)
            for row in procedure_rows:
                base_procedures.append(
                    BaseProcedure(
                        name=row.name,
                        language=row.language.upper() if row.language else "",
                        argument_signature=row.arguments,
                        return_type=None,
                        procedure_definition=row.definition,
                        created=None,
                        last_altered=None,
                        comment=row.comment,
                        extra_properties=None,
                    )
                )
            return base_procedures

    def close(self) -> None:
        if self.sql_aggregator:
            self.sql_aggregator.close()
        super().close()
