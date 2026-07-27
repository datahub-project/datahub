"""Configuration classes for Fabric OneLake connector."""

from typing import Literal, Optional

from pydantic import Field, model_validator

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig


class ExtractSchemaConfig(ConfigModel):
    """Configuration for schema extraction."""

    enabled: bool = Field(default=True, description="Enable schema extraction")
    method: Literal["sql_analytics_endpoint"] = Field(
        default="sql_analytics_endpoint",
        description=(
            "Schema extraction method. Currently only 'sql_analytics_endpoint' is supported."
        ),
    )


class SqlEndpointConfig(ConfigModel):
    """Configuration for SQL Analytics Endpoint schema extraction.

    References:
    - https://learn.microsoft.com/en-us/fabric/data-warehouse/warehouse-connectivity
    - https://learn.microsoft.com/en-us/fabric/data-warehouse/connect-to-fabric-data-warehouse
    - https://learn.microsoft.com/en-us/fabric/data-warehouse/what-is-the-sql-analytics-endpoint-for-a-lakehouse
    - https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver17#encrypt
    """

    enabled: bool = Field(
        default=True, description="Enable SQL Analytics Endpoint connection"
    )
    odbc_driver: str = Field(
        default="ODBC Driver 18 for SQL Server",
        description="ODBC driver name for SQL Server connections.",
    )
    encrypt: Literal["yes", "no", "mandatory", "optional", "strict"] = Field(
        default="yes",
        description=(
            "Enable encryption for SQL Server connections. "
            "Valid values: 'yes'/'mandatory' (enable encryption, default in ODBC Driver 18.0+), "
            "'no'/'optional' (disable encryption), or 'strict' (ODBC Driver 18.0+, TDS 8.0 protocol only, "
            "always verifies server certificate). "
            "See: https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver17#encrypt"
        ),
    )
    trust_server_certificate: Literal["yes", "no"] = Field(
        default="no",
        description=(
            "Trust server certificate without validation. "
            "Set to 'yes' only if certificate validation fails. "
            "When 'encrypt=strict', this setting is ignored and certificate validation is always performed. "
            "See: https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver17"
        ),
    )

    query_timeout: int = Field(
        default=30,
        description="Timeout for SQL queries in seconds",
        ge=1,
        le=300,
    )


class FabricUsageConfig(BaseUsageConfig):
    """Usage tracking configuration for Fabric OneLake.

    Retention is 30 days per Microsoft Fabric documentation:
    https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights
    """

    include_usage_statistics: bool = Field(
        default=True,
        description=(
            "Master toggle for Fabric usage extraction. When False, no "
            "`datasetUsageStatistics` or `operation` aspects are emitted from "
            "queryinsights, regardless of `include_operational_stats`."
        ),
    )

    skip_failed_queries: bool = Field(
        default=True,
        description=(
            "When True, the SQL filter excludes rows where status != 'Succeeded' "
            "(canceled / failed queries are skipped at the source)."
        ),
    )

    include_queries: bool = Field(
        default=True,
        description=(
            "If enabled, emit a `Query` entity for each unique parsed queryinsights "
            "row so the SQL becomes a first-class, searchable asset in DataHub "
            "(visible on the Queries tab and as standalone Query pages). "
            "Requires `include_usage_statistics=True`."
        ),
    )


class FabricOneLakeSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    """Configuration for Fabric OneLake source.

    This connector extracts metadata from Microsoft Fabric OneLake including:
    - Workspaces as Containers
    - Lakehouses as Containers
    - Warehouses as Containers
    - Schemas as Containers
    - Tables as Datasets with schema metadata

    Note on Tenant/Platform Instance:
    The Fabric REST API does not expose tenant-level endpoints or operations.
    All API operations are performed at the workspace level. To represent tenant-level
    organization in DataHub, users should set the `platform_instance` configuration
    field to their tenant identifier (e.g., "contoso-tenant"). This will be included
    in all container and dataset URNs, effectively grouping all workspaces under the
    specified platform instance/tenant.
    """

    # Azure Authentication
    credential: AzureCredentialConfig = Field(
        default_factory=AzureCredentialConfig,
        description=(
            "Azure authentication configuration. Supports service principal, "
            "managed identity, Azure CLI, or auto-detection (DefaultAzureCredential). "
            "See AzureCredentialConfig for detailed options."
        ),
    )

    # Filtering options
    workspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter workspaces by name. "
            "Example: allow=['prod-.*'], deny=['.*-test']"
        ),
    )

    lakehouse_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter lakehouses by name. "
            "Applied to all workspaces matching workspace_pattern."
        ),
    )

    warehouse_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter warehouses by name. "
            "Applied to all workspaces matching workspace_pattern."
        ),
    )

    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter tables by name. "
            "Format: 'schema.table' or just 'table' for default schema."
        ),
    )

    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter views by name. "
            "Format: 'schema.view_name' or just 'view_name' for default schema."
        ),
    )

    # Feature flags
    extract_lakehouses: bool = Field(
        default=True,
        description="Whether to extract lakehouses and their tables.",
    )

    extract_warehouses: bool = Field(
        default=True,
        description="Whether to extract warehouses and their tables.",
    )

    extract_views: bool = Field(
        default=True,
        description=(
            "Whether to extract views and their definitions. "
            "Requires a configured sql_endpoint, because views are discovered via "
            "INFORMATION_SCHEMA.VIEWS over the SQL Analytics Endpoint."
        ),
    )

    extract_schemas: bool = Field(
        default=True,
        description="Whether to extract schema containers. "
        "If False, tables will be directly under lakehouse/warehouse containers.",
    )

    # API timeout
    api_timeout: int = Field(
        default=30,
        description="Timeout for REST API calls in seconds.",
        ge=1,
        le=300,
    )

    # Schema extraction configuration
    extract_schema: ExtractSchemaConfig = Field(
        default_factory=ExtractSchemaConfig,
        description="Configuration for schema extraction from tables.",
    )

    # SQL Analytics Endpoint configuration
    sql_endpoint: Optional[SqlEndpointConfig] = Field(
        default_factory=SqlEndpointConfig,
        description="SQL Analytics Endpoint configuration. "
        "Required when extract_views=True or when extract_schema.enabled=True with "
        "method='sql_analytics_endpoint'.",
    )

    # Stateful Ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Configuration for stateful ingestion and stale entity removal. "
            "When enabled, tracks ingested entities and removes those that "
            "no longer exist in Fabric."
        ),
    )

    # Usage tracking
    usage: FabricUsageConfig = Field(
        default_factory=FabricUsageConfig,
        description=(
            "Usage tracking configuration. Reads `queryinsights.exec_requests_history` "
            "on each Lakehouse/Warehouse SQL Analytics Endpoint. Requires a "
            "configured and enabled `sql_endpoint`."
        ),
    )

    @model_validator(mode="after")
    def validate_sql_endpoint_dependencies(self):
        """sql_endpoint must be configured when any feature that uses it is enabled.

        View discovery (INFORMATION_SCHEMA.VIEWS), column-level schema extraction
        (INFORMATION_SCHEMA.COLUMNS), and usage statistics
        (queryinsights.exec_requests_history) all go through the SQL Analytics
        Endpoint but are otherwise independent.
        """
        sql_endpoint_enabled = (
            self.sql_endpoint is not None and self.sql_endpoint.enabled
        )
        if sql_endpoint_enabled:
            return self

        requiring_features = []
        if self.extract_views:
            requiring_features.append("extract_views=True")
        if (
            self.extract_schema.enabled
            and self.extract_schema.method == "sql_analytics_endpoint"
        ):
            requiring_features.append(
                "extract_schema with method='sql_analytics_endpoint'"
            )
        if self.usage.include_usage_statistics:
            requiring_features.append("usage.include_usage_statistics=True")

        if requiring_features:
            raise ValueError(
                f"sql_endpoint must be configured with enabled=True when any of "
                f"the following are set: {', '.join(requiring_features)}. "
                f"These features all query the SQL Analytics Endpoint."
            )
        return self
