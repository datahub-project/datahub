import time
from typing import List

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.types import ExecutionRequestSchedule, FetcherConfig


class Fetcher:
    """Interface for fetching work items to execute."""

    def __init__(self, graph: DataHubGraph, config: FetcherConfig):
        """
        Initialize the MonitorFetcher with the URL of the API.

        :param graph: An instance of the DataHubGraph (API Client) object.
        """
        self.graph = graph
        self.config = config
        self.last_updated = time.time()

    def fetch_execution_requests(self) -> List[ExecutionRequestSchedule]:
        raise NotImplementedError

    def touch(self) -> None:
        self.last_updated = time.time()

    def alive(self) -> bool:
        return time.time() < (self.last_updated + self.config.refresh_interval * 60 * 2)
