import logging
import weakref
from typing import Any, Optional

from databricks import sql
from databricks.sql.client import Connection as DatabricksSqlConnection
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.constants import DATABRICKS_PLATFORM_URN
from datahub_executor.common.exceptions import SourceConnectionErrorException

logger = logging.getLogger(__name__)

# User agent entry for Databricks connections
DATABRICKS_USER_AGENT_ENTRY = "datahub"


class DatabricksConnection(Connection):
    """A connection to Databricks with proper lifecycle management"""

    config: UnityCatalogSourceConfig

    def __init__(self, urn: str, config: UnityCatalogSourceConfig, graph: DataHubGraph):
        super().__init__(urn, DATABRICKS_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[DatabricksSqlConnection] = None
        # Register cleanup when this object is garbage collected
        self._finalizer = weakref.finalize(
            self, self._cleanup_connection, self.connection
        )

    @staticmethod
    def _cleanup_connection(connection: Optional[DatabricksSqlConnection]) -> None:
        """Static method to clean up connection resources"""
        if connection is not None:
            try:
                connection.close()
                logger.debug("Databricks connection closed during cleanup")
            except Exception as e:
                logger.warning(
                    f"Error closing Databricks connection during cleanup: {e}"
                )

    def get_client(self) -> DatabricksSqlConnection:
        """Get the Databricks SQL connection, creating it if necessary"""
        try:
            # Check if connection is None or closed
            if self.connection is None or (
                hasattr(self.connection, "_closed") and self.connection._closed
            ):
                if self.connection is not None:
                    logger.debug("Reconnecting to Databricks (connection was closed)")
                self.connection = sql.connect(
                    server_hostname=self._get_server_hostname(),
                    http_path=f"/sql/1.0/warehouses/{self.config.warehouse_id}",
                    access_token=self.config.token,
                    user_agent_entry=DATABRICKS_USER_AGENT_ENTRY,
                )
                # Update the finalizer with the new connection
                self._finalizer.detach()
                self._finalizer = weakref.finalize(
                    self, self._cleanup_connection, self.connection
                )
            return self.connection
        except Exception as e:
            logger.error(f"Failed to create Databricks connection: {e}")
            raise SourceConnectionErrorException(
                message="Unable to connect to databricks instance.",
                connection_urn=DATABRICKS_PLATFORM_URN,
            )

    def close(self) -> None:
        """Explicitly close the connection"""
        if self.connection is not None:
            try:
                self.connection.close()
                logger.debug("Databricks connection explicitly closed")
            except Exception as e:
                logger.warning(f"Error closing Databricks connection: {e}")
            finally:
                self.connection = None
                self._finalizer.detach()  # Prevent cleanup since we handled it

    def __enter__(self) -> "DatabricksConnection":
        """Context manager entry"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - ensures connection is closed"""
        self.close()

    def _get_server_hostname(self) -> str:
        return self.config.workspace_url.rstrip("/").removeprefix("https://")
