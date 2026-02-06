"""Databricks Unity Catalog connection configuration."""

from typing import Any, Dict, Optional
from urllib.parse import urlparse

import pydantic
from databricks.sdk import WorkspaceClient
from pydantic import Field, SecretStr, model_validator

from datahub._version import nice_version_name
from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.unity.azure_auth_config import AzureAuthConfig

DATABRICKS = "databricks"
# User agent entry for Databricks connections.
# Keep this stable to avoid needing to coordinate version bumps across internal components.
DATABRICKS_USER_AGENT_ENTRY = "datahub"


class UnityCatalogConnectionConfig(ConfigModel):
    """
    Configuration for connecting to Databricks Unity Catalog.
    Contains only connection-related fields that can be reused across different sources.
    """

    scheme: str = DATABRICKS
    token: Optional[str] = pydantic.Field(
        default=None, description="Databricks personal access token"
    )
    azure_auth: Optional[AzureAuthConfig] = Field(
        default=None, description="Azure configuration"
    )
    client_id: Optional[str] = pydantic.Field(
        default=None, description="Databricks service principal client ID"
    )
    client_secret: Optional[SecretStr] = pydantic.Field(
        default=None, description="Databricks service principal client secret"
    )
    workspace_url: str = pydantic.Field(
        description="Databricks workspace url. e.g. https://my-workspace.cloud.databricks.com"
    )
    warehouse_id: Optional[str] = pydantic.Field(
        default=None,
        description=(
            "SQL Warehouse id, for running queries. Must be explicitly provided to enable SQL-based features. "
            "Required for the following features that need SQL access: "
            "1) Tag extraction (include_tags=True) - queries system.information_schema.tags "
            "2) Hive Metastore catalog (include_hive_metastore=True) - queries legacy hive_metastore catalog "
            "3) System table lineage (lineage_data_source=SYSTEM_TABLES) - queries system.access.table_lineage/column_lineage "
            "4) Data profiling (profiling.enabled=True) - runs SELECT/ANALYZE queries on tables. "
            "When warehouse_id is missing, these features will be automatically disabled (with warnings) to allow ingestion to continue."
        ),
    )

    extra_client_options: Dict[str, Any] = Field(
        default={},
        description="Additional options to pass to Databricks SQLAlchemy client.",
    )

    def __init__(self, **data: Any):
        super().__init__(**data)

    def get_sql_alchemy_url(self, database: Optional[str] = None) -> str:
        uri_opts = {}
        if database:
            uri_opts["catalog"] = database
        return make_sqlalchemy_uri(
            scheme=self.scheme,
            username=None,
            password=None,
            at=urlparse(self.workspace_url).netloc,
            db=database,
            uri_opts=uri_opts,
        )

    def get_options(self) -> dict:
        return self.extra_client_options

    @model_validator(mode="before")
    def at_most_one_auth_method_provided(cls, values: dict) -> dict:
        token = bool(values.get("token"))
        azure_auth = bool(values.get("azure_auth"))
        client_oauth = bool(values.get("client_id") or values.get("client_secret"))
        # Check if at most one of the authentication methods is provided
        if sum([token, azure_auth, client_oauth]) > 1:
            raise ValueError(
                "More than one of 'token', 'azure_auth', and 'client_id'/'client_secret' provided."
                " Please provide only one authentication method."
            )
        return values


def create_workspace_client(config: UnityCatalogConnectionConfig) -> WorkspaceClient:
    workspace_client = WorkspaceClient(
        host=config.workspace_url,
        product=DATABRICKS_USER_AGENT_ENTRY,
        product_version=nice_version_name(),
        token=config.token,
        azure_tenant_id=config.azure_auth.tenant_id if config.azure_auth else None,
        azure_client_id=config.azure_auth.client_id if config.azure_auth else None,
        azure_client_secret=(
            config.azure_auth.client_secret.get_secret_value()
            if config.azure_auth
            else None
        ),
        client_id=config.client_id,
        client_secret=config.client_secret.get_secret_value()
        if config.client_secret
        else None,
    )
    if config.warehouse_id:
        # workspace_client.config.warehouse_id isn't populated by the WorkspaceClient
        # initialization, only by environment variable. But we want it populated so
        # we can generate the SQL HTTP Path later.
        workspace_client.config.warehouse_id = config.warehouse_id
    return workspace_client


def get_sql_connection_params(workspace_client: WorkspaceClient) -> dict[str, Any]:
    return {
        "server_hostname": workspace_client.config.host.replace("https://", ""),
        "user_agent_entry": DATABRICKS_USER_AGENT_ENTRY,
        # Don't use workspace_client.config.sql_http_path because it adds
        # `sdk-feature/sql-http-path` to the user-agent, which causes errors from the
        # Databricks endpoint: `{user_agent} is not supported for SQL warehouses`.
        "http_path": f"/sql/1.0/warehouses/{workspace_client.config.warehouse_id}",
        # Delegate to workspace_client for authentication, since it (1) already handles
        # all auth methods and (2) automatically handles token expiration and refresh.
        # The `credentials_provider` parameter must be a factory function which is called
        # once upon initialization, returning another function which is called on every
        # request, which returns authentication headers for the Databricks endpoints.
        "credentials_provider": lambda: workspace_client.config.authenticate,
    }
