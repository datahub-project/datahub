import logging
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Mapping,
    Optional,
    Sequence,
)

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
    parent_required,
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

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from datahub.ingestion.agent.sql_passthrough import QueryBudget

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
    # Read by name: default_databases is declared only by sources that drop
    # databases, and callers outside SQLCommonConfig pass their own configs.
    defaults = getattr(config, hook, None)
    return callable(defaults) and name.lower() in {d.lower() for d in defaults()}


def _qualifying_container(
    config: ConfigModel, parent_path: Sequence[str]
) -> Optional[str]:
    """The container a schema name is qualified with, or None.

    A Qualifier(authoritative=True) field wins (the recipe reads that one
    container whatever --parent says); then the caller's --parent, since a
    recipe may span several; then a Qualifier field pinning a single one.
    """
    # lazy: agent.introspect is only needed once a probe runs
    from datahub.ingestion.agent.introspect import declared_qualifier

    declared, authoritative = declared_qualifier(config)
    if authoritative and declared:
        return declared
    if parent_path:
        return parent_path[-1]
    return declared


_NO_CONTAINER_WARNING = (
    "no parent container given, so these were judged on "
    "'schema.entity'; this source matches a fully qualified name, so "
    "pass the containing database/project to get the verdict "
    "ingestion actually makes"
)


def qualified_table_target(
    container: Optional[str], schema: str, entity: str, warn: Callable[[str], None]
) -> Optional[str]:
    """`container.schema.entity`: what a source whose tables live under a
    database or project matches table_pattern and view_pattern against.

    None after warning when the container is unknown, leaving the shim to
    judge `schema.entity`: an invented container would judge another
    database's object. The warning names no object, so it shows once.
    """
    if container:
        return f"{container}.{schema}.{entity}"
    warn(_NO_CONTAINER_WARNING)
    return None


def _qualified_schema_verdict(
    config: ConfigModel, ctx: VerdictContext
) -> Optional[Verdict]:
    """The verdict on `container.schema`, which is what ingestion matches
    schema_pattern against once match_fully_qualified_names is on."""
    if not getattr(config, "match_fully_qualified_names", False):
        return None
    container = _qualifying_container(config, ctx.parent_path)
    if container is None:
        # The bare name is judged against a pattern written for qualified
        # names. Names no object, so it shows once.
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


@dataclass(frozen=True)
class ProbeEngineSettings:
    """What a dialect adds to the engine the probe builds from a recipe.

    Declared by the dialect's config (SQLCommonConfig.probe_engine_settings):
    a connect_arg the driver rejects stops the connection opening at all.
    """

    # Merged over the recipe's connect_args key by key; a setting that defers
    # to or extends the recipe's is composed by the config.
    connect_args: Mapping[str, Any] = field(default_factory=dict)
    # Run on the built engine, before probe_prepare_engine, for what
    # connect_args cannot carry: a statement issued on each new connection.
    prepare: Optional[Callable[["Engine"], None]] = None
    # Whether these bound every probe statement by the budget's timeout. When
    # False the probe reports no time ceiling rather than an unenforced one.
    timeout_applies: bool = False


def recipe_connect_args(config: "SQLCommonConfig") -> Mapping[str, Any]:
    """The connect_args the recipe passes create_engine, as ingestion does."""
    return config.options.get("connect_args") or {}


def probe_label_connect_arg(config: "SQLCommonConfig", kwarg: str) -> Dict[str, str]:
    """`{kwarg: PROBE_QUERY_LABEL}`, so probe traffic is told apart from
    ingestion's in the server's own logs, or nothing when the recipe already
    names its connection through `kwarg`: that name is the recipe's choice."""
    if kwarg in recipe_connect_args(config):
        return {}
    # lazy: ingestion importing this module does not need the probe framework
    from datahub.ingestion.agent.sql_passthrough import PROBE_QUERY_LABEL

    return {kwarg: PROBE_QUERY_LABEL}


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

    # --- Probe hooks: see docs/dev_guides/probe_interface.md ---
    # Schemas the source drops whatever schema_pattern says (system catalogs).
    # A subclass returns ingestion's own list.
    @classmethod
    def default_schemas(cls) -> FrozenSet[str]:
        return frozenset()

    def probe_match_target(self, ctx: ClassifyContext) -> Optional[str]:
        """The identifier ingestion matches a table or view against, or None
        to judge the bare name.

        From the connector's own get_identifier through sql_probe's shim,
        which consults probe_filter_target first. Containers and top-level
        kinds keep the bare name (a qualified schema is
        probe_verdict_override's, which reports its own target).
        """
        if ctx.kind in (
            DatasetContainerSubTypes.SCHEMA,
            DatasetContainerSubTypes.DATABASE,
        ):
            return None
        if self.probe_ancestor_kinds(ctx.kind) == ():
            return None
        # Without the container the shim builds ".orders", which ingestion
        # never matches.
        if parent_required(ctx):
            return None
        # lazy: sql_probe imports sql_common, which imports this module
        from datahub.ingestion.source.sql.sql_probe import _identifier_target

        target = _identifier_target(ctx)
        if not isinstance(target, str) or not target:
            # Names no object, so it shows once.
            ctx.warn(
                "the connector's identifier resolver returned nothing usable, so "
                "these were judged on their bare names; the verdict may not be "
                "the one ingestion makes"
            )
            return None
        if target.startswith(".") or ".." in target:
            # A component is missing; per object, so it names the identifier.
            ctx.warn(
                f"could not build a complete identifier for '{ctx.name}' (got "
                f"'{target}'); judged on its bare name instead"
            )
            return None
        return target

    def probe_filter_target(
        self,
        schema: str,
        entity: str,
        warn: Callable[[str], None],
        database: Optional[str] = None,
    ) -> Optional[str]:
        """The exact string ingestion filters table_pattern/view_pattern
        against, for a connector whose identifier the get_identifier shim
        cannot build: its Source is not a SQLAlchemySource, or its
        get_identifier reads state ingestion sets while it walks. None (the
        default) lets the shim resolve it.

        `database` is the container above the schema when the caller gave
        one; always passed by keyword. Where only the caller knows the
        container, return qualified_table_target(database, ...). `warn`
        reports a less precise fallback, deduplicated by message.
        """
        return None

    @classmethod
    def probe_kind_switches(cls) -> Mapping[str, str]:
        """Tables and views are emitted only while these are on. The other
        include_* flags decide what is emitted about an object, not whether.
        A subclass adds its own to super().probe_kind_switches()."""
        return {
            str(DatasetSubTypes.TABLE): "include_tables",
            str(DatasetSubTypes.VIEW): "include_views",
        }

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        """See sql_structural_verdict, which an override of this one calls
        for the names its own rules leave alone."""
        return sql_structural_verdict(self, ctx)

    @classmethod
    def probe_kind_overrides(cls) -> Mapping[str, str]:
        """`containers` lists schemas or databases, by tier: see
        probe_container_kind. A subclass adding a kind of its own adds it to
        super().probe_kind_overrides()."""
        return {"containers": str(cls.probe_container_kind())}

    @classmethod
    def probe_container_kind(cls) -> str:
        """What `containers` (get_schema_names) returns on this source: Schema
        on three-tier sources, Database on two-tier ones. Read by
        probe_kind_overrides and probe_ancestor_kinds, so they agree."""
        return DatasetContainerSubTypes.SCHEMA

    def probe_ancestor_kinds(self, kind: str) -> Optional[Sequence[str]]:
        """The container kinds above `kind`, outermost first: Database then
        Schema on a three-tier source, Database alone on a two-tier one."""
        chain = (
            (DatasetContainerSubTypes.DATABASE, DatasetContainerSubTypes.SCHEMA)
            if self.probe_container_kind() == DatasetContainerSubTypes.SCHEMA
            else (DatasetContainerSubTypes.DATABASE,)
        )
        return ancestors_in(chain, kind, (DatasetSubTypes.TABLE, DatasetSubTypes.VIEW))

    @classmethod
    def probe_catalog_scope(cls) -> CatalogScope:
        """What `probe sql` may read on this dialect: information_schema only
        by default. A dialect whose catalog lives elsewhere overrides it,
        naming relations rather than whole schemas (see CatalogScope).

        Read only by SqlAlchemyMetadataProbe.for_config, as the provider's
        `catalog_scope`. A connector with a provider of its own declares
        `catalog_scope` on that class instead.
        """
        return CatalogScope()

    def probe_engine_settings(self, budget: "QueryBudget") -> ProbeEngineSettings:
        """The statement ceiling and client label this dialect's driver takes.

        Nothing by default: declare only settings the driver is known to
        accept, since a wrong connect_arg stops the connection opening.
        """
        return ProbeEngineSettings()

    def probe_prepare_engine(self, engine: Any) -> None:
        """Apply connection-time setup that a bare create_engine() would miss.

        The probe builds its own engine rather than the connector's Source, so
        whatever the Source does to its engine (replacing the dialect, say) is
        skipped unless declared here. Keep it to setup that is safe without a
        report or a running pipeline.
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
