import logging
from abc import abstractmethod
from typing import Any, Dict, Optional

import pydantic
from pydantic import Field
from sqlalchemy.engine import URL

from datahub.configuration.common import AllowDenyPattern, ConfigModel, LineageConfig
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled

logger: logging.Logger = logging.getLogger(__name__)


class SQLCommonConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    LineageConfig,
    ClassificationSourceConfigMixin,
):
    options: dict = pydantic.Field(
        default_factory=dict,
        description="Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.",
    )
    # Although the 'table_pattern' enables you to skip everything from certain schemas,
    # having another option to allow/deny on schema level is an optimization for the case when there is a large number
    # of schemas that one wants to skip and you want to avoid the time to needlessly fetch those tables only to filter
    # them out afterwards via the table_pattern.
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered.",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Attach domains to databases, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "Marketing".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.',
    )

    include_views: Optional[bool] = Field(
        default=True, description="Whether views should be ingested."
    )
    include_tables: Optional[bool] = Field(
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

    profiling: GEProfilingConfig = GEProfilingConfig()
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    @pydantic.root_validator(pre=True)
    def view_pattern_is_table_pattern_unless_specified(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        view_pattern = values.get("view_pattern")
        table_pattern = values.get("table_pattern")
        if table_pattern and not view_pattern:
            logger.info(f"Applying table_pattern {table_pattern} to view_pattern.")
            values["view_pattern"] = table_pattern
        return values

    @pydantic.root_validator(skip_on_failure=True)
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling: Optional[GEProfilingConfig] = values.get("profiling")
        # Note: isinstance() check is required here as unity-catalog source reuses
        # SQLCommonConfig with different profiling config than GEProfilingConfig
        if (
            profiling is not None
            and isinstance(profiling, GEProfilingConfig)
            and profiling.enabled
        ):
            profiling._allow_deny_patterns = values["profile_pattern"]
        return values

    @abstractmethod
    def get_sql_alchemy_url(self):
        pass


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
        description="URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.",
    )

    # Duplicate of SQLCommonConfig.options
    options: dict = pydantic.Field(
        default_factory=dict,
        description=(
            "Any options specified here will be passed to "
            "[SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs."
            " To set connection arguments in the URL, specify them under `connect_args`."
        ),
    )

    _database_alias_removed = pydantic_removed_field("database_alias")

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


def make_sqlalchemy_uri(
    scheme: str,
    username: Optional[str],
    password: Optional[str],
    at: Optional[str],
    db: Optional[str],
    uri_opts: Optional[Dict[str, Any]] = None,
) -> str:
    host: Optional[str] = None
    port: Optional[int] = None
    if at:
        try:
            host, port_str = at.rsplit(":", 1)
            port = int(port_str)
        except ValueError:
            host = at
            port = None
    if uri_opts:
        uri_opts = {k: v for k, v in uri_opts.items() if v is not None}

    return str(
        URL.create(
            drivername=scheme,
            username=username,
            password=password,
            host=host,
            port=port,
            database=db,
            query=uri_opts or {},
        )
    )
