"""Databricks Unity Catalog connection configuration."""

from typing import Any, Dict, Optional
from urllib.parse import urlparse

import pydantic
from pydantic import Field

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

    def get_options(self) -> dict:
        return self.extra_client_options
