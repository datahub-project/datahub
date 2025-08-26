import logging
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from snowflake.connector import SnowflakeConnection as NativeSnowflakeConnection

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.constants import SNOWFLAKE_PLATFORM_URN

logger = logging.getLogger(__name__)


class SnowflakeConnection(Connection):
    """A connection to Snowflake"""

    def __init__(
        self, urn: str, config: SnowflakeConnectionConfig, graph: DataHubGraph
    ):
        super().__init__(urn, SNOWFLAKE_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[NativeSnowflakeConnection] = None

    def get_client(self) -> NativeSnowflakeConnection:
        # TODO: Add try
        # TODO: Filter out unsupported auth types.
        if self.connection is None:
            self.connection = self.config.get_native_connection()
        return self.connection
