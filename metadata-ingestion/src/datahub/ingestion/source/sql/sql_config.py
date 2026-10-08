import logging
from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    FrozenSet,
    Mapping,
    Optional,
    Sequence,
)

import pydantic
from pydantic import Field, model_validator
from typing_extensions import Annotated

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    Enables,
    Filters,
)
from datahub.configuration.source_common import (
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
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

# Re-exported: connectors import ProbeEngineSettings from this module.
from datahub.ingestion.source.sql.protocol_probe_settings import (
    ProbeEngineSettings as ProbeEngineSettings,
    probe_settings_for_url,
)
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled

if TYPE_CHECKING:
    from datahub.ingestion.agent.sql_gate import CatalogScope
    from datahub.ingestion.agent.sql_passthrough import QueryBudget
    from datahub.ingestion.agent.verdicts import (
        ClassifyContext,
        Verdict,
        VerdictContext,
    )

# The probe framework is imported only inside the probe hooks below, never at
# module level: every SQL ingestion source imports this module, and would pay
# for the framework (and depend on it importing cleanly) without ever probing.

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

    # --- Probe hooks: see docs/dev_guides/probe_interface.md ---
    def probe_verdict_override(self, ctx: "VerdictContext") -> Optional["Verdict"]:
        """See sql_structural_verdict, which an override of this one calls
        for the names its own rules leave alone."""
        return sql_structural_verdict(self, ctx)


def sql_structural_verdict(
    config: ConfigModel, ctx: "VerdictContext"
) -> Optional["Verdict"]:
    """The SQL family's structural verdicts (see
    sql_probe_verdicts.sql_structural_verdict): SQLFilterConfig's
    probe_verdict_override, and what a config with verdict rules of its own
    returns for the names its rules leave alone."""
    # lazy: keeps the probe framework out of every SQL source's import
    from datahub.ingestion.source.sql import sql_probe_verdicts

    return sql_probe_verdicts.sql_structural_verdict(config, ctx)


# The hooks source/sql/ reads off a SQLCommonConfig, beyond the framework's
# own (probe_methods.CONFIG_HOOKS): the guide's SQL-family table, which
# test_probe_contract checks against this list. Only a SQLCommonConfig
# subclass may declare one.
SQL_FAMILY_HOOKS: FrozenSet[str] = frozenset(
    {
        # sql_probe._identifier_target: the identifier for a source whose
        # identifier the get_identifier shim cannot build.
        "probe_filter_target",
        # sqlalchemy_probe._container_normalizer: how a listed container is
        # spelled for ingestion.
        "probe_normalize_container",
        # sqlalchemy_probe.for_config: the statement ceiling, client label and
        # engine setup, the sqlglot dialect `probe sql` parses as, the URL
        # the probe dials when it differs from get_sql_alchemy_url(), and
        # what `probe sql` may read (CatalogScope, set as the provider's
        # catalog_scope for the gate).
        "probe_engine_settings",
        "probe_sqlglot_dialect",
        "probe_sql_alchemy_url",
        "probe_catalog_scope",
        # SQLCommonConfig: whether `containers` lists schemas or databases,
        # which its probe_kind_overrides and ancestor chain follow.
        "probe_container_kind",
    }
)


class SQLCommonConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    IncrementalLineageConfigMixin,
    ClassificationSourceConfigMixin,
    SQLFilterConfig,
):
    # So the framework's unknown-hook check knows a SQL config's own hooks
    # (probe_methods.CONFIG_HOOK_FAMILY_ATTRIBUTE).
    __probe_family_hooks__: ClassVar[FrozenSet[str]] = SQL_FAMILY_HOOKS

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

    # Tables and views are emitted only while these are on. The other
    # include_* flags decide what is emitted about an object, not whether.
    include_views: Annotated[bool, Enables(DatasetSubTypes.VIEW)] = Field(
        default=True, description="Whether views should be ingested."
    )
    include_tables: Annotated[bool, Enables(DatasetSubTypes.TABLE)] = Field(
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

    def probe_match_target(self, ctx: "ClassifyContext") -> Optional[str]:
        """The identifier ingestion matches a table or view against, or None
        to judge the bare name: the connector's own get_identifier, through
        sql_probe's shim, which consults probe_filter_target first."""
        # lazy: sql_probe imports sql_common, which imports this module
        from datahub.ingestion.source.sql.sql_probe import sql_table_match_target

        return sql_table_match_target(self, ctx)

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

    def probe_normalize_container(self, name: str) -> str:
        """A listed container in the spelling ingestion matches on, for a
        connector whose Inspector spells it differently: callers pass
        `containers` output back as --parent. The name unchanged by default."""
        return name

    def probe_sql_alchemy_url(self) -> str:
        """The URL the probe dials, for a connector whose ingestion dials
        another than get_sql_alchemy_url() (Doris names its catalog per
        call). get_sql_alchemy_url() by default."""
        return str(self.get_sql_alchemy_url())

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
        # lazy: keeps the probe framework out of every SQL source's import
        from datahub.ingestion.agent.verdicts import ancestors_in

        return ancestors_in(chain, kind, (DatasetSubTypes.TABLE, DatasetSubTypes.VIEW))

    @classmethod
    def probe_catalog_scope(cls) -> "CatalogScope":
        """What `probe sql` may read on this dialect: information_schema only
        by default. A dialect whose catalog lives elsewhere overrides it,
        naming relations rather than whole schemas (see CatalogScope).

        Read only by SqlAlchemyMetadataProbe.for_config, as the provider's
        `catalog_scope`. A connector with a provider of its own declares
        `catalog_scope` on that class instead.
        """
        # lazy: keeps the probe framework out of every SQL source's import
        from datahub.ingestion.agent.sql_gate import (
            SESSION_TEXT_RELATIONS,
            CatalogScope,
        )

        return CatalogScope(excluded_relations=SESSION_TEXT_RELATIONS)

    def probe_engine_settings(self, budget: "QueryBudget") -> ProbeEngineSettings:
        """The statement ceiling, client label and engine setup of the
        probe's engine.

        By default the settings of the wire protocol the probe URL names
        (protocol_probe_settings.probe_settings_for_url), so a config whose
        sqlalchemy_uri names another protocol's dialect gets that one's. The
        probe builds its own engine rather than the connector's Source, so
        whatever the Source does to its engine (replacing the dialect, say)
        is skipped unless added here: return
        super().probe_engine_settings(budget).followed_by(step), keeping to
        setup that is safe without a report or a running pipeline.
        """
        return probe_settings_for_url(self, budget)

    @classmethod
    def probe_sqlglot_dialect(cls) -> Optional[str]:
        """The sqlglot dialect `probe sql` parses this source's queries as,
        or None (the default) for the engine dialect's own name, mapped where
        SQLAlchemy and sqlglot spell it differently (sqlalchemy_probe). A
        dialect that table does not name and sqlglot spells differently
        declares it here; otherwise `sql` refuses every query."""
        return None

    @classmethod
    def probe_provider_class(cls) -> type:
        # lazy: sqlalchemy_probe imports this module
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
