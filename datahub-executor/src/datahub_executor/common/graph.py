import logging

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

logger = logging.getLogger(__name__)


class DataHubAssertionGraph(DataHubGraph):
    """An extension of the DataHubGraph class that provides additional functionality for Assertion evaluation"""

    def __init__(self, config: DatahubClientConfig) -> None:
        super().__init__(config)
