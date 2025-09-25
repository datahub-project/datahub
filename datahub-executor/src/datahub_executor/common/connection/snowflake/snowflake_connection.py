import logging
import weakref
from typing import Any, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from snowflake.connector import SnowflakeConnection as NativeSnowflakeConnection

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.constants import SNOWFLAKE_PLATFORM_URN

logger = logging.getLogger(__name__)


class SnowflakeConnection(Connection):
    """A connection to Snowflake with proper lifecycle management"""

    def __init__(
        self, urn: str, config: SnowflakeConnectionConfig, graph: DataHubGraph
    ):
        super().__init__(urn, SNOWFLAKE_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[NativeSnowflakeConnection] = None
        # Register cleanup when this object is garbage collected
        self._finalizer = weakref.finalize(
            self, self._cleanup_connection, self.connection
        )

    @staticmethod
    def _cleanup_connection(connection: Optional[NativeSnowflakeConnection]) -> None:
        """Static method to clean up connection resources"""
        if connection is not None:
            try:
                connection.close()
                logger.debug("Snowflake connection closed during cleanup")
            except Exception as e:
                logger.warning(
                    f"Error closing Snowflake connection during cleanup: {e}"
                )

    def get_client(self) -> NativeSnowflakeConnection:
        """Get the native Snowflake connection, creating it if necessary"""
        try:
            if self.connection is None or self.connection.is_closed():
                if self.connection is not None:
                    logger.debug("Reconnecting to Snowflake (connection was closed)")
                self.connection = self.config.get_native_connection()
                # Update the finalizer with the new connection
                self._finalizer.detach()
                self._finalizer = weakref.finalize(
                    self, self._cleanup_connection, self.connection
                )
            return self.connection
        except Exception as e:
            logger.error(f"Failed to create Snowflake connection: {e}")
            raise

    def close(self) -> None:
        """Explicitly close the connection"""
        if self.connection is not None:
            try:
                self.connection.close()
                logger.debug("Snowflake connection explicitly closed")
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {e}")
            finally:
                self.connection = None
                self._finalizer.detach()  # Prevent cleanup since we handled it

    def __enter__(self) -> "SnowflakeConnection":
        """Context manager entry"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - ensures connection is closed"""
        self.close()
