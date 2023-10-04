import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

import pydantic
from pydantic import Field
from typing_extensions import Literal

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.source.ge_data_profiler import DATABRICKS
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig, make_sqlalchemy_uri
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulProfilingConfigMixin,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.operation_config import (
    OperationConfig,
    is_profiling_enabled,
)


class UnityCatalogConfig(ConfigModel):
    method: str

    warehouse_id: Optional[str] = Field(
        default=None, description="SQL Warehouse id, for running profiling queries."
    )

    pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter tables for profiling during ingestion. "
            "Specify regex to match the `catalog.schema.table` format. "
            "Note that only tables allowed by the `table_pattern` will be considered."
        ),
    )


class UnityCatalogAnalyzeProfilerConfig(UnityCatalogConfig):
    method: Literal["analyze"] = "analyze"

    # TODO: Reduce duplicate code with DataLakeProfilerConfig, GEProfilingConfig, SQLAlchemyConfig
    enabled: bool = Field(
        default=False, description="Whether profiling should be done."
    )
    operation_config: OperationConfig = Field(
        default_factory=OperationConfig,
        description="Experimental feature. To specify operation configs.",
    )

    profile_table_level_only: bool = Field(
        default=False,
        description="Whether to perform profiling at table-level only or include column-level profiling as well.",
    )

    call_analyze: bool = Field(
        default=True,
        description=(
            "Whether to call ANALYZE TABLE as part of profile ingestion."
            "If false, will ingest the results of the most recent ANALYZE TABLE call, if any."
        ),
    )

    max_wait_secs: int = Field(
        default=int(timedelta(hours=1).total_seconds()),
        description="Maximum time to wait for an ANALYZE TABLE query to complete.",
    )

    max_workers: int = Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for profiling. Set to 1 to disable.",
    )

    @pydantic.root_validator
    def warehouse_id_required_for_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        if values.get("enabled") and not values.get("warehouse_id"):
            raise ValueError("warehouse_id must be set when profiling is enabled.")
        return values

    @property
    def include_columns(self):
        return not self.profile_table_level_only


class UnityCatalogGEProfilerConfig(UnityCatalogConfig, GEProfilingConfig):
    method: Literal["ge"] = "ge"

    max_wait_secs: Optional[int] = Field(
        default=None,
        description="Maximum time to wait for a table to be profiled.",
    )


class UnityCatalogSourceConfig(
    SQLCommonConfig,
    StatefulIngestionConfigBase,
    BaseUsageConfig,
    DatasetSourceConfigMixin,
    StatefulProfilingConfigMixin,
):
    token: str = pydantic.Field(description="Databricks personal access token")
    workspace_url: str = pydantic.Field(
        description="Databricks workspace url. e.g. https://my-workspace.cloud.databricks.com"
    )
    workspace_name: Optional[str] = pydantic.Field(
        default=None,
        description="Name of the workspace. Default to deployment name present in workspace_url",
    )

    ingest_data_platform_instance_aspect: Optional[bool] = pydantic.Field(
        default=False,
        description="Option to enable/disable ingestion of the data platform instance aspect. The default data platform instance id for a dataset is workspace_name",
    )

    _only_ingest_assigned_metastore_removed = pydantic_removed_field(
        "only_ingest_assigned_metastore"
    )

    _metastore_id_pattern_removed = pydantic_removed_field("metastore_id_pattern")

    catalog_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for catalogs to filter in ingestion. Specify regex to match the full `metastore.catalog` name.",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to the full `metastore.catalog.schema` name. e.g. to match all tables in schema analytics, use the regex `^mymetastore\\.mycatalog\\.analytics$`.",
    )

    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in `catalog.schema.table` format. e.g. to match all tables starting with customer in Customer catalog and public schema, use the regex `Customer\\.public\\.customer.*`.",
    )

    include_table_lineage: bool = pydantic.Field(
        default=True,
        description="Option to enable/disable lineage generation.",
    )

    include_notebooks: bool = pydantic.Field(
        default=False,
        description="Ingest notebooks, represented as DataHub datasets.",
    )

    include_ownership: bool = pydantic.Field(
        default=False,
        description="Option to enable/disable ownership generation for metastores, catalogs, schemas, and tables.",
    )

    _rename_table_ownership = pydantic_renamed_field(
        "include_table_ownership", "include_ownership"
    )

    include_column_lineage: bool = pydantic.Field(
        default=True,
        description="Option to enable/disable lineage generation. Currently we have to call a rest call per column to get column level lineage due to the Databrick api which can slow down ingestion. ",
    )

    include_usage_statistics: bool = Field(
        default=True,
        description="Generate usage statistics.",
    )

    profiling: Union[UnityCatalogGEProfilerConfig, UnityCatalogAnalyzeProfilerConfig] = Field(  # type: ignore
        default=UnityCatalogGEProfilerConfig(),
        description="Data profiling configuration",
        discriminator="method",
    )

    scheme: str = DATABRICKS

    def get_sql_alchemy_url(self):
        return make_sqlalchemy_uri(
            scheme=self.scheme,
            username="token",
            password=self.token,
            at=urlparse(self.workspace_url).netloc,
            db=None,
            uri_opts={
                "http_path": f"/sql/1.0/warehouses/{self.profiling.warehouse_id}"
            },
        )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    def is_ge_profiling(self) -> bool:
        return self.profiling.method == "ge"

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Unity Catalog Stateful Ingestion Config."
    )

    @pydantic.validator("start_time")
    def within_thirty_days(cls, v: datetime) -> datetime:
        if (datetime.now(timezone.utc) - v).days > 30:
            raise ValueError("Query history is only maintained for 30 days.")
        return v

    @pydantic.validator("workspace_url")
    def workspace_url_should_start_with_http_scheme(cls, workspace_url: str) -> str:
        if not workspace_url.lower().startswith(("http://", "https://")):
            raise ValueError(
                "Workspace URL must start with http scheme. e.g. https://my-workspace.cloud.databricks.com"
            )
        return workspace_url
