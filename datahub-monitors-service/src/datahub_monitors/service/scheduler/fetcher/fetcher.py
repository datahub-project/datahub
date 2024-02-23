from typing import List

from datahub.ingestion.graph.client import DataHubGraph

from datahub_monitors.service.scheduler.fetcher.config import FetcherConfig
from datahub_monitors.service.scheduler.types import ExecutionRequestSchedule


class Fetcher:
    """Interface for fetching work items to execute."""

    def __init__(self, graph: DataHubGraph, config: FetcherConfig):
        """
        Initialize the MonitorFetcher with the URL of the API.

        :param graph: An instance of the DataHubGraph (API Client) object.
        """
        self.graph = graph
        self.config = config

    def fetch_execution_requests(self) -> List[ExecutionRequestSchedule]:
        raise NotImplementedError
