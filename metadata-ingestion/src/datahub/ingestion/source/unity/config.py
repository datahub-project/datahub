import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import pydantic
from pydantic import Field
from typing_extensions import Literal

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
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
from datahub.utilities.global_warning_util import add_global_warning

logger = logging.getLogger(__name__)


class UnityCatalogProfilerConfig(ConfigModel):
    method: str = Field(
        description=(
            "Profiling method to use."
            " Options supported are `ge` and `analyze`."
            " `ge` uses Great Expectations and runs SELECT SQL queries on profiled tables."
            " `analyze` calls ANALYZE TABLE on profiled tables. Only works for delta tables."
        ),
    )

    # TODO: Support cluster compute as well, for ge profiling
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


class DeltaLakeDetails(ConfigModel):
    platform_instance_name: Optional[str] = Field(
        default=None, description="Delta-lake paltform instance name"
    )
    env: str = Field(default="PROD", description="Delta-lake environment")


class UnityCatalogAnalyzeProfilerConfig(UnityCatalogProfilerConfig):
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

    @property
    def include_columns(self):
        return not self.profile_table_level_only


class UnityCatalogGEProfilerConfig(UnityCatalogProfilerConfig, GEProfilingConfig):
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
    LowerCaseDatasetUrnConfigMixin,
):
    token: str = pydantic.Field(description="Databricks personal access token")
    workspace_url: str = pydantic.Field(
        description="Databricks workspace url. e.g. https://my-workspace.cloud.databricks.com"
    )
    warehouse_id: Optional[str] = pydantic.Field(
        default=None,
        description="SQL Warehouse id, for running queries. If not set, will use the default warehouse.",
    )
    include_hive_metastore: bool = pydantic.Field(
        default=True,
        description="Whether to ingest legacy `hive_metastore` catalog. This requires executing queries on SQL warehouse.",
    )
    workspace_name: Optional[str] = pydantic.Field(
        default=None,
        description="Name of the workspace. Default to deployment name present in workspace_url",
    )

    include_metastore: bool = pydantic.Field(
        default=False,
        description=(
            "Whether to ingest the workspace's metastore as a container and include it in all urns."
            " Changing this will affect the urns of all entities in the workspace."
            " This config is deprecated and will be removed in the future,"
            " so it is recommended to not set this to `True` for new ingestions."
            " If you have an existing unity catalog ingestion, you'll want to avoid duplicates by soft deleting existing data."
            " If stateful ingestion is enabled, running with `include_metastore: false` should be sufficient."
            " Otherwise, we recommend deleting via the cli: `datahub delete --platform databricks` and re-ingesting with `include_metastore: false`."
        ),
    )

    ingest_data_platform_instance_aspect: Optional[bool] = pydantic.Field(
        default=False,
        description=(
            "Option to enable/disable ingestion of the data platform instance aspect."
            " The default data platform instance id for a dataset is workspace_name"
        ),
    )

    _only_ingest_assigned_metastore_removed = pydantic_removed_field(
        "only_ingest_assigned_metastore"
    )

    _metastore_id_pattern_removed = pydantic_removed_field("metastore_id_pattern")

    catalogs: Optional[List[str]] = pydantic.Field(
        default=None,
        description=(
            "Fixed list of catalogs to ingest."
            " If not specified, catalogs will be ingested based on `catalog_pattern`."
        ),
    )

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

    notebook_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for notebooks to filter in ingestion, based on notebook *path*."
            " Specify regex to match the entire notebook path in `/<dir>/.../<name>` format."
            " e.g. to match all notebooks in the root Shared directory, use the regex `/Shared/.*`."
        ),
    )

    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Attach domains to catalogs, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "Marketing".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.',
    )

    include_table_lineage: bool = pydantic.Field(
        default=True,
        description="Option to enable/disable lineage generation.",
    )

    include_external_lineage: bool = pydantic.Field(
        default=True,
        description=(
            "Option to enable/disable lineage generation for external tables."
            " Only external S3 tables are supported at the moment."
        ),
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

    column_lineage_column_limit: int = pydantic.Field(
        default=300,
        description="Limit the number of columns to get column level lineage. ",
    )

    lineage_max_workers: int = pydantic.Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for column lineage thread pool executor. Set to 1 to disable.",
        hidden_from_docs=True,
    )

    include_usage_statistics: bool = Field(
        default=True,
        description="Generate usage statistics.",
    )

    # TODO: Remove `type:ignore` by refactoring config
    profiling: Union[UnityCatalogGEProfilerConfig, UnityCatalogAnalyzeProfilerConfig] = Field(  # type: ignore
        default=UnityCatalogGEProfilerConfig(),
        description="Data profiling configuration",
        discriminator="method",
    )

    emit_siblings: bool = pydantic.Field(
        default=True,
        description="Whether to emit siblings relation with corresponding delta-lake platform's table. If enabled, this will also ingest the corresponding delta-lake table.",
    )

    delta_lake_options: DeltaLakeDetails = Field(
        default=DeltaLakeDetails(),
        description="Details about the delta lake, incase to emit siblings",
    )

    scheme: str = DATABRICKS

    def get_sql_alchemy_url(self, database: Optional[str] = None) -> str:
        uri_opts = {"http_path": f"/sql/1.0/warehouses/{self.warehouse_id}"}
        if database:
            uri_opts["catalog"] = database
        return make_sqlalchemy_uri(
            scheme=self.scheme,
            username="token",
            password=self.token,
            at=urlparse(self.workspace_url).netloc,
            db=database,
            uri_opts=uri_opts,
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

    @pydantic.validator("include_metastore")
    def include_metastore_warning(cls, v: bool) -> bool:
        if v:
            msg = (
                "`include_metastore` is enabled."
                " This is not recommended and this option will be removed in the future, which is a breaking change."
                " All databricks urns will change if you re-ingest with this disabled."
                " We recommend soft deleting all databricks data and re-ingesting with `include_metastore` set to `False`."
            )
            logger.warning(msg)
            add_global_warning(msg)
        return v

    @pydantic.root_validator(skip_on_failure=True)
    def set_warehouse_id_from_profiling(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        profiling: Optional[
            Union[UnityCatalogGEProfilerConfig, UnityCatalogAnalyzeProfilerConfig]
        ] = values.get("profiling")
        if not values.get("warehouse_id") and profiling and profiling.warehouse_id:
            values["warehouse_id"] = profiling.warehouse_id
        if (
            values.get("warehouse_id")
            and profiling
            and profiling.warehouse_id
            and values["warehouse_id"] != profiling.warehouse_id
        ):
            raise ValueError(
                "When `warehouse_id` is set, it must match the `warehouse_id` in `profiling`."
            )

        if values.get("include_hive_metastore") and not values.get("warehouse_id"):
            raise ValueError(
                "When `include_hive_metastore` is set, `warehouse_id` must be set."
            )

        if values.get("warehouse_id") and profiling and not profiling.warehouse_id:
            profiling.warehouse_id = values["warehouse_id"]

        if profiling and profiling.enabled and not profiling.warehouse_id:
            raise ValueError("warehouse_id must be set when profiling is enabled.")

        return values

    @pydantic.validator("schema_pattern", always=True)
    def schema_pattern_should__always_deny_information_schema(
        cls, v: AllowDenyPattern
    ) -> AllowDenyPattern:
        v.deny.append(".*\\.information_schema")
        return v
