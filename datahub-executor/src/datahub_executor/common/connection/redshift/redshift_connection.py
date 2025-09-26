import logging
import weakref
from typing import Any, Optional

import redshift_connector
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.redshift.config import RedshiftConfig

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.constants import REDSHIFT_PLATFORM_URN
from datahub_executor.config import DATAHUB_APPNAME

logger = logging.getLogger(__name__)


class RedshiftConnection(Connection):
    """A connection to Redshift with proper lifecycle management"""

    def __init__(self, urn: str, config: RedshiftConfig, graph: DataHubGraph):
        super().__init__(urn, REDSHIFT_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[Any] = None
        # Register cleanup when this object is garbage collected
        self._finalizer = weakref.finalize(
            self, self._cleanup_connection, self.connection
        )

    @staticmethod
    def _cleanup_connection(connection: Optional[Any]) -> None:
        """Static method to clean up connection resources"""
        if connection is not None:
            try:
                connection.close()
                logger.debug("Redshift connection closed during cleanup")
            except Exception as e:
                logger.warning(f"Error closing Redshift connection during cleanup: {e}")

    def get_client(self) -> Any:
        """Get the Redshift connection, creating it if necessary"""
        try:
            if self.connection is None or self.connection.closed:
                if self.connection is not None:
                    logger.debug("Reconnecting to Redshift (connection was closed)")
                client_options = self.config.extra_client_options
                host, port = self.config.host_port.split(":")
                self.connection = redshift_connector.connect(
                    application_name=f"datahub-executor.{DATAHUB_APPNAME}",
                    host=host,
                    port=int(port),
                    user=self.config.username,
                    database=self.config.database,
                    password=(
                        self.config.password.get_secret_value()
                        if self.config.password
                        else None
                    ),
                    **client_options,
                )
                self.connection.autocommit = True
                # Update the finalizer with the new connection
                self._finalizer.detach()
                self._finalizer = weakref.finalize(
                    self, self._cleanup_connection, self.connection
                )
            return self.connection
        except Exception as e:
            logger.error(f"Failed to create Redshift connection: {e}")
            raise

    def close(self) -> None:
        """Explicitly close the connection"""
        if self.connection is not None:
            try:
                self.connection.close()
                logger.debug("Redshift connection explicitly closed")
            except Exception as e:
                logger.warning(f"Error closing Redshift connection: {e}")
            finally:
                self.connection = None
                self._finalizer.detach()  # Prevent cleanup since we handled it

    def __enter__(self) -> "RedshiftConnection":
        """Context manager entry"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - ensures connection is closed"""
        self.close()
