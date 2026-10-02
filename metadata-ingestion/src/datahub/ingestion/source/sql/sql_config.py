import logging
from abc import abstractmethod
from typing import Any, Callable, Dict, FrozenSet, Mapping, Optional, Sequence

import pydantic
from pydantic import Field, model_validator
from typing_extensions import Annotated

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.configuration.source_common import (
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.agent.pattern_path import pattern_at
from datahub.ingestion.agent.sql_gate import CatalogScope
from datahub.ingestion.agent.verdicts import (
    ClassifyContext,
    Verdict,
    VerdictContext,
    ancestors_in,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.profiling.config import ProfilingConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled

logger: logging.Logger = logging.getLogger(__name__)


class SQLFilterConfig(ConfigModel):
    # Although the 'table_pattern' enables you to skip everything from certain schemas,
    # having another option to allow/deny on schema level is an optimization for the case when there is a large number
    # of schemas that one wants to skip and you want to avoid the time to needlessly fetch those tables only to filter
    # them out afterwards via the table_pattern.
    schema_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.SCHEMA)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )
    table_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    view_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.VIEW)] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )

    @model_validator(mode="before")
    @classmethod
    def view_pattern_is_table_pattern_unless_specified(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        view_pattern = values.get("view_pattern")
        table_pattern = values.get("table_pattern")
        if table_pattern and not view_pattern:
            logger.info(f"Applying table_pattern {table_pattern} to view_pattern.")
            values["view_pattern"] = table_pattern
        return values


_NEEDS_PARENT_WARNING = (
    "this source matches containers on a qualified name and could not tell "
    "which one you mean, so these were judged on their bare names and will "
    "mostly read as excluded; pass --parent to get the verdict ingestion "
    "actually makes"
)


def _in_defaults(config: ConfigModel, hook: str, name: str) -> bool:
    # Read by name: default_databases is declared only by the sources that
    # drop databases (Postgres, SQL Server), and callers outside
    # SQLCommonConfig pass their own configs.
    defaults = getattr(config, hook, None)
    return callable(defaults) and name.lower() in {d.lower() for d in defaults()}


def _qualifying_container(
    config: ConfigModel, parent_path: Sequence[str]
) -> Optional[str]:
    """The container a schema name is qualified with, or None.

    A Qualifier(authoritative=True) field wins: Redshift reads one database
    per recipe, so a --parent naming another is not what ingestion reads.
    Otherwise the caller's --parent, since a recipe may span several
    databases or projects; then a field pinning a single one.
    """
    # lazy: agent.introspect is only needed once a probe runs
    from datahub.ingestion.agent.introspect import declared_qualifier

    declared, authoritative = declared_qualifier(config)
    if authoritative and declared:
        return declared
    if parent_path:
        return parent_path[-1]
    return declared


def _qualified_schema_verdict(
    config: ConfigModel, ctx: VerdictContext
) -> Optional[Verdict]:
    """The verdict on `container.schema`, which is what ingestion matches
    schema_pattern against once match_fully_qualified_names is on."""
    if not getattr(config, "match_fully_qualified_names", False):
        return None
    container = _qualifying_container(config, ctx.parent_path)
    if container is None:
        # The bare name is judged instead, against a pattern written for
        # qualified names, so most names read as excluded. Not naming the
        # object: ctx.warn dedupes by message, so this is reported once.
        ctx.warn(_NEEDS_PARENT_WARNING)
        return None
    if ctx.pattern_field is None:
        return None
    pattern = pattern_at(config, ctx.pattern_field)
    if pattern is None:
        return None
    included = is_schema_allowed(pattern, ctx.name, container, True)
    return Verdict(
        included=included,
        excluded_by=None if included else ctx.pattern_field,
        matched_target=f"{container}.{ctx.name}",
    )


def sql_structural_verdict(
    config: ConfigModel, ctx: VerdictContext
) -> Optional[Verdict]:
    """The SQL family's verdicts that no single allow/deny pattern states.

    A database in default_databases() or a schema in default_schemas() is
    dropped whatever the pattern says: ingestion never lists those system
    catalogs. A schema on a source with match_fully_qualified_names on is
    judged as `container.schema`, and the verdict reports that string as its
    target. None leaves the pattern to decide.

    SQLCommonConfig's probe_verdict_override; a config with rules of its own
    returns this for the names those rules leave alone. A kind-switch
    exclusion already in ctx.structural stands, so this returns None for it.
    """
    if ctx.structural is not None:
        return None
    if ctx.kind == DatasetContainerSubTypes.DATABASE:
        if _in_defaults(config, "default_databases", ctx.name):
            return Verdict(False, "default_database")
        return None
    if ctx.kind != DatasetContainerSubTypes.SCHEMA:
        return None
    if _in_defaults(config, "default_schemas", ctx.name):
        return Verdict(False, "default_schema")
    return _qualified_schema_verdict(config, ctx)


class SQLCommonConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    IncrementalLineageConfigMixin,
    ClassificationSourceConfigMixin,
    SQLFilterConfig,
):
    options: dict = pydantic.Field(
        default_factory=dict,
        description="Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine) as kwargs.",
    )
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered.",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Attach domains to databases, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "Marketing".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.',
    )

    include_views: bool = Field(
        default=True, description="Whether views should be ingested."
    )
    include_tables: bool = Field(
        default=True, description="Whether tables should be ingested."
    )

    include_table_location_lineage: bool = Field(
        default=True,
        description="If the source supports it, include table lineage to the underlying storage location.",
    )

    include_view_lineage: bool = Field(
        default=True,
        description="Populates view->view and table->view lineage using DataHub's sql parser.",
    )

    include_view_column_lineage: bool = Field(
        default=True,
        description="Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser."
        " Requires `include_view_lineage` to be enabled.",
    )

    use_file_backed_cache: bool = Field(
        default=True,
        description="Whether to use a file backed cache for the view definitions.",
    )

    profiling: ProfilingConfig = ProfilingConfig()
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    @model_validator(mode="after")
    def ensure_profiling_pattern_is_passed_to_profiling(self):
        profiling = self.profiling
        # Note: isinstance() check is required here as unity-catalog source reuses
        # SQLCommonConfig with different profiling config than ProfilingConfig
        if (
            profiling is not None
            and isinstance(profiling, ProfilingConfig)
            and profiling.enabled
        ):
            profiling._allow_deny_patterns = self.profile_pattern
        return self

    @abstractmethod
    def get_sql_alchemy_url(self):
        pass

    # --- Agent probe contract (see datahub.ingestion.agent.probe_methods) ---
    # Generic SQL sources are a 2-level namespace (schema -> table -> column).
    # Database-aware sources (Snowflake, BigQuery) override these.
    # Schemas the source drops regardless of schema_pattern (system catalogs like
    # information_schema / pg_catalog). Empty by default; subclasses override to
    # reuse their own list — see RedshiftConfig.default_schemas().
    @classmethod
    def default_schemas(cls) -> FrozenSet[str]:
        return frozenset()

    def probe_match_target(self, ctx: ClassifyContext) -> str:
        """The exact string ingestion filters this node's pattern against.

        Routes to sql_probe's get_identifier shim, so the connection-free
        `probe filter` path and the hierarchy walk resolve the identifier the
        same single way. Distinct from probe_filter_target below, which is the
        per-connector *override* that shim consults first.
        """
        from datahub.ingestion.source.sql.sql_probe import _identifier_target

        return _identifier_target(ctx)

    def probe_filter_target(
        self,
        schema: str,
        entity: str,
        warn: Callable[[str], None],
        database: Optional[str] = None,
    ) -> Optional[str]:
        """Override point for a connector whose real Source doesn't extend
        SQLAlchemySource, so sql_probe.py's generic get_identifier shim (see
        sql_probe._identifier_target) has no get_identifier to call for it.
        Return the exact string ingestion filters table_pattern/view_pattern
        against, or None (the default) to let that shim keep resolving it.
        Checked before the shim on every SQL Table-level node.
        UnityCatalogSourceConfig is the only override left: where the container
        is pinned by a config field, Qualifier states that declaratively and
        the shim resolves the rest, which is how Redshift, Snowflake and
        BigQuery stopped needing one.

        `database` is the container above the schema when the caller supplied
        one -- parent_path[0] on a source whose hierarchy has a level above
        the schema. Redshift and Unity Catalog take theirs from config
        instead and ignore this; Snowflake and BigQuery cannot, because one
        recipe spans several databases/projects and only the caller knows
        which one the node came from.

        `warn` reports a degrade -- an override that cannot return its exact
        ingestion identifier and is falling back to something less precise
        (see UnityCatalogSourceConfig's override, the only one that calls it
        today) -- onto the same ProbeMethodResult.warnings list ProbeSoftError
        feeds. It dedupes by message, so a connector-wide reason called once
        per node classified in a level is only recorded once per probe call.
        """
        return None

    @classmethod
    def probe_kind_switches(cls) -> Mapping[str, str]:
        """Tables and views are emitted only while these are on.

        Only these two: the other include_* flags (include_view_lineage,
        include_table_location_lineage, ...) decide what else is emitted
        about an object, not whether the object is. A subclass with a switch
        of its own adds it to super().probe_kind_switches().
        """
        return {
            str(DatasetSubTypes.TABLE): "include_tables",
            str(DatasetSubTypes.VIEW): "include_views",
        }

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        """See sql_structural_verdict, which an override of this one calls
        for the names its own rules leave alone."""
        return sql_structural_verdict(self, ctx)

    @classmethod
    def probe_container_kind(cls) -> str:
        """What `containers` returns on this source: Schema, or Database.

        The same Inspector call (get_schema_names) means different things per tier --
        three-tier sources return schemas filtered by schema_pattern, two-tier ones
        return databases filtered by database_pattern, where schema_pattern is
        deprecated. One provider class serves both, so the class cannot say which;
        the recipe's config can.
        """
        return DatasetContainerSubTypes.SCHEMA

    def probe_ancestor_kinds(self, kind: str) -> Optional[Sequence[str]]:
        """The container kinds above `kind`, outermost first.

        `probe filter` judges the --parent containers with these, because
        ingestion never reaches a table whose schema or database its patterns
        exclude -- judging the table's own pattern alone reported it included.
        A three-tier source nests schemas in databases; a two-tier one has
        databases only (see probe_container_kind).
        """
        chain = (
            (DatasetContainerSubTypes.DATABASE, DatasetContainerSubTypes.SCHEMA)
            if self.probe_container_kind() == DatasetContainerSubTypes.SCHEMA
            else (DatasetContainerSubTypes.DATABASE,)
        )
        return ancestors_in(chain, kind, (DatasetSubTypes.TABLE, DatasetSubTypes.VIEW))

    @classmethod
    def probe_catalog_scope(cls) -> CatalogScope:
        """What `probe sql` may read on this dialect.

        The default is `information_schema` and nothing else, which is correct for
        the standard dialects and safe for the rest. A dialect whose catalog lives
        elsewhere overrides this -- Oracle and Teradata have no information_schema
        at all, so without an override their `sql` command can answer nothing.

        Name relations rather than whole schemas for a vendor catalog: see
        CatalogScope's docstring for why that is not merely stylistic.

        Overridden by a provider that sets `catalog_scope` on itself, which is
        what a bespoke per-connector provider does: Snowflake and BigQuery each
        build their own client rather than going through the shared
        SqlAlchemyMetadataProbe, so there is no generic adapter that needs to
        read the value back off a config. The shared adapter serves ~15
        dialects and therefore must, which is why the method exists here at all.

        For Snowflake there is a second reason: SnowflakeSummaryConfig is not a
        SQLCommonConfig, so it could not carry this method even if it wanted to.
        That does not apply to BigQuery -- BigQueryV2Config is a SQLCommonConfig
        -- and an earlier version of this comment wrongly gave it as the reason
        for both.

        Declaring it in both places means this one is dead; a contract test
        refuses that rather than leaving it to be discovered.
        """
        return CatalogScope()

    def probe_prepare_engine(self, engine: Any) -> None:
        """Apply connection-time setup that a bare create_engine() would miss.

        The probe builds its own engine rather than constructing the connector's
        Source, which fires ingestion telemetry and wants a PipelineContext. That
        keeps the probe cheap and side-effect free, at the cost of skipping
        whatever the Source does to its engine after building it -- and some
        connectors do a lot. Athena replaces the dialect outright, because
        PyAthena's own omits ICEBERG from get_table_names and mis-parses complex
        column types.

        A no-op by default, because most dialects need nothing. Override it where
        the connector's own engine is not a plain one, and keep it to setup that
        is safe without a report or a running pipeline.
        """
        return None

    @classmethod
    def probe_provider_class(cls) -> type:
        from datahub.ingestion.source.sql.sqlalchemy_probe import (
            SqlAlchemyMetadataProbe,
        )

        return SqlAlchemyMetadataProbe


class SQLAlchemyConnectionConfig(ConfigModel):
    username: Optional[str] = Field(default=None, description="username")
    password: Optional[pydantic.SecretStr] = Field(
        default=None, exclude=True, description="password"
    )
    host_port: str = Field(description="host URL")
    database: Optional[str] = Field(default=None, description="database (catalog)")

    scheme: str = Field(description="scheme")
    sqlalchemy_uri: Optional[str] = Field(
        default=None,
        description="URI of database to connect to. See https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls. Takes precedence over other connection parameters.",
    )

    # Duplicate of SQLCommonConfig.options
    options: dict = pydantic.Field(
        default_factory=dict,
        description=(
            "Any options specified here will be passed to "
            "[SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine) as kwargs."
            " To set connection arguments in the URL, specify them under `connect_args`."
        ),
    )

    _database_alias_removed = pydantic_removed_field(
        "database_alias", month="November", year=2023
    )

    def get_sql_alchemy_url(
        self, uri_opts: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> str:
        if not ((self.host_port and self.scheme) or self.sqlalchemy_uri):
            raise ValueError("host_port and schema or connect_uri required.")

        return self.sqlalchemy_uri or make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password.get_secret_value() if self.password is not None else None,
            self.host_port,
            database or self.database,
            uri_opts=uri_opts,
        )


class BasicSQLAlchemyConfig(SQLAlchemyConnectionConfig, SQLCommonConfig):
    pass
