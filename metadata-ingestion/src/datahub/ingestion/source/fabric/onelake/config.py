"""Configuration classes for Fabric OneLake connector."""

from typing import Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class FabricOneLakeSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
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

    # Feature flags
    extract_lakehouses: bool = Field(
        default=True,
        description="Whether to extract lakehouses and their tables.",
    )

    extract_warehouses: bool = Field(
        default=True,
        description="Whether to extract warehouses and their tables.",
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

    # TODO: Schema extraction via SQL endpoint
    # The Fabric REST API does not provide column schema information in the Table model.
    # To extract table schemas, we need to use the SQL Analytics endpoint:
    # 1. Configure sql_endpoint_connection_string_template with {workspace_id} and {item_id} placeholders
    # 2. Query INFORMATION_SCHEMA.COLUMNS using pyodbc
    # 3. Requires pyodbc library and proper authentication
    # See client.py for implementation details

    # Stateful Ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Configuration for stateful ingestion and stale entity removal. "
            "When enabled, tracks ingested entities and removes those that "
            "no longer exist in Fabric."
        ),
    )
