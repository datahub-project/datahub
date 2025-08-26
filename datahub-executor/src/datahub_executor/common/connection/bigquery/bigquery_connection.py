import logging
from typing import Any, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.constants import BIGQUERY_PLATFORM_URN

logger = logging.getLogger(__name__)


class BigQueryConnection(Connection):
    """A connection to BigQuery"""

    def __init__(self, urn: str, config: BigQueryV2Config, graph: DataHubGraph):
        super().__init__(urn, BIGQUERY_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[Any] = None

    def get_client(self) -> Any:
        if self.connection is None:
            self.connection = self.config.get_bigquery_client()
        return self.connection
