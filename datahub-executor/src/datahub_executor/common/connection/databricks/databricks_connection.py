from typing import Optional

from databricks import sql
from databricks.sql.client import Connection as DatabricksSqlConnection
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.constants import DATABRICKS_PLATFORM_URN
from datahub_executor.common.exceptions import SourceConnectionErrorException


class DatabricksConnection(Connection):
    """A connection to Databricks"""

    config: UnityCatalogSourceConfig

    def __init__(self, urn: str, config: UnityCatalogSourceConfig, graph: DataHubGraph):
        super().__init__(urn, DATABRICKS_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[DatabricksSqlConnection] = None

    def get_client(self) -> DatabricksSqlConnection:
        if self.connection is None:
            try:
                self.connection = sql.connect(
                    server_hostname=self._get_server_hostname(),
                    http_path=f"/sql/1.0/warehouses/{self.config.warehouse_id}",
                    access_token=self.config.token,
                )
            except Exception:
                raise SourceConnectionErrorException(
                    message="Unable to connect to databricks instance.",
                    connection_urn=DATABRICKS_PLATFORM_URN,
                )

        return self.connection

    def _get_server_hostname(self) -> str:
        return self.config.workspace_url.rstrip("/").removeprefix("https://")
