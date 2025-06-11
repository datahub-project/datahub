import logging
from urllib.parse import quote_plus

from datahub.configuration.common import PermissiveConfigModel
from pydantic import (
    Field,
)

logger = logging.getLogger(__name__)


class UnityCatalogConnectionConfig(PermissiveConfigModel):
    workspace_url: str = Field(
        description="The hostname of the Databricks SQL endpoint. If not provided, it will be derived from the account ID.",
        examples=["https://my-databricks-account.cloud.databricks.com"],
    )

    warehouse_id: str = Field(
        description="The ID of the Databricks SQL warehouse to use. If not provided, the default warehouse will be used.",
        examples=["abcd1234efgh5678"],
    )

    token: str = Field(
        description="The access token for authenticating with the Databricks API.",
        examples=["dapi123456789abcdef0123456789abcdef"],
    )

    @property
    def server_hostname(self) -> str:
        """
        Returns the server hostname for the Unity Catalog connection.
        """
        return self.workspace_url.replace("https://", "")

    @property
    def http_path(self) -> str:
        """
        Returns the HTTP path for the Unity Catalog connection.
        """
        return f"/sql/1.0/warehouses/{self.warehouse_id}"

    def create_databricks_connection_string(self) -> str:
        """
        Create a Databricks connection string for SQLAlchemy.

        Args:
            server_hostname: Databricks server hostname
            http_path: HTTP path to the cluster/warehouse
            access_token: Databricks access token
            catalog: Optional default catalog
            schema: Optional default schema

        Returns:
            SQLAlchemy connection string
        """
        # URL encode the access token
        encoded_token = quote_plus(self.token)

        # Base connection string (database name is required, use 'default' or your schema)
        conn_str = f"databricks+connector://token:{encoded_token}@{self.server_hostname}:443/default"
        # Add http_path, catalog, and schema as query parameters
        params = [f"http_path={quote_plus(self.http_path)}"]

        if params:
            conn_str += "?" + "&".join(params)

        return conn_str
