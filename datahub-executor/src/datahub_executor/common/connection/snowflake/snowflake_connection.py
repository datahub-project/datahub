import logging
from typing import Any, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source_config.sql.snowflake import SnowflakeConfig

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.constants import SNOWFLAKE_PLATFORM_URN

logger = logging.getLogger(__name__)


class SnowflakeConnection(Connection):
    """A connection to Snowflake"""

    def __init__(self, urn: str, config: SnowflakeConfig, graph: DataHubGraph):
        super().__init__(urn, SNOWFLAKE_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[Any] = None

    def get_client(self) -> Any:
        # TODO: Add try
        # TODO: Filter out unsupported auth types.
        if self.connection is None:
            self.connection = self.config.get_connection()
        return self.connection
