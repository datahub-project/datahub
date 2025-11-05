"""Databricks Unity Catalog connection configuration."""

from typing import Any, Dict, Optional
from urllib.parse import urlparse

import pydantic
from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri

DATABRICKS = "databricks"


class UnityCatalogConnectionConfig(ConfigModel):
    """
    Configuration for connecting to Databricks Unity Catalog.
    Contains only connection-related fields that can be reused across different sources.

    Authentication can be configured via:
    1. Personal access token (legacy): Set `token` field
    2. OAuth (unified auth): Omit `token` and configure via environment variables or
       `.databrickscfg` file following Databricks unified authentication.
       See https://docs.databricks.com/dev-tools/auth/unified-auth
    """

    scheme: str = DATABRICKS
    token: Optional[str] = pydantic.Field(
        default=None,
        description=(
            "Databricks personal access token (legacy). "
            "If not provided, OAuth authentication will be attempted using "
            "Databricks unified authentication (environment variables or .databrickscfg file)."
        ),
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

    def _resolve_token(self) -> str:
        """
        Resolve authentication token using the following priority:
        1. Explicit token from config
        2. Token from Databricks SDK Config (environment variables or .databrickscfg)
        3. OAuth token via service principal
        """
        if self.token:
            return self.token

        try:
            from databricks.sdk.core import Config, oauth_service_principal

            if self.workspace_url:
                host = self.workspace_url.replace("https://", "").replace("http://", "")
                config = Config(host=host)
            else:
                config = Config()

            if config.token:
                return config.token

            try:
                principal = oauth_service_principal(config)
                oauth_token = principal.oauth_token()
                if oauth_token and oauth_token.access_token:
                    return oauth_token.access_token
            except Exception:
                pass

        except ImportError:
            pass
        except Exception:
            pass

        raise ValueError(
            "No authentication token found. "
            "Either provide 'token' in config, or configure Databricks unified authentication "
            "via environment variables or .databrickscfg file. "
            "See https://docs.databricks.com/dev-tools/auth/unified-auth"
        )

    def get_sql_alchemy_url(self, database: Optional[str] = None) -> str:
        token = self._resolve_token()
        uri_opts = {"http_path": f"/sql/1.0/warehouses/{self.warehouse_id}"}
        if database:
            uri_opts["catalog"] = database
        return make_sqlalchemy_uri(
            scheme=self.scheme,
            username="token",
            password=token,
            at=urlparse(self.workspace_url).netloc,
            db=database,
            uri_opts=uri_opts,
        )

    def get_options(self) -> dict:
        return self.extra_client_options
