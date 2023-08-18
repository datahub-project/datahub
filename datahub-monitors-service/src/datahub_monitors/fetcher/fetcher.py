import logging
from typing import List

from datahub.ingestion.graph.client import DataHubGraph
from tenacity import retry, stop_after_attempt, wait_exponential

from datahub_monitors.constants import LIST_MONITORS_BATCH_SIZE
from datahub_monitors.fetcher.mapper import graphql_to_monitors
from datahub_monitors.fetcher.types import MonitorFetcherConfig
from datahub_monitors.fetcher.util import build_filters
from datahub_monitors.graphql.query import GRAPHQL_LIST_MONITORS_QUERY
from datahub_monitors.types import Monitor

logger = logging.getLogger(__name__)


class MonitorFetcher:
    """Class used to fetch monitors from an external API."""

    def __init__(self, graph: DataHubGraph, config: MonitorFetcherConfig):
        """
        Initialize the MonitorFetcher with the URL of the API.

        :param graph: An instance of the DataHubGraph (API Client) object.
        """
        self.graph = graph
        self.config = config

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=10)
    )
    def fetch_monitors(self) -> List[Monitor]:
        """
        Fetch the list of monitors from the API.

        :return: The list of monitors.
        """
        result = self.graph.execute_graphql(
            GRAPHQL_LIST_MONITORS_QUERY,
            variables={
                "input": {
                    "types": ["MONITOR"],
                    "start": 0,
                    "count": LIST_MONITORS_BATCH_SIZE,
                    "query": "*",
                    "searchFlags": {"skipCache": True},
                    "orFilters": build_filters(self.config),
                }
            },
        )

        if "error" in result and result["error"] is not None:
            # TODO: add either logging or throwing here.
            logger.error(
                f"Received error while fetching monitors from GMS! {result.get('error')}"
            )
            return []

        if (
            "searchAcrossEntities" not in result
            or "searchResults" not in result["searchAcrossEntities"]
        ):
            logger.error(
                "Found incomplete search results when fetching monitors from GMS!"
            )
            return []

        return graphql_to_monitors(
            [
                entity["entity"]
                for entity in result["searchAcrossEntities"]["searchResults"]
            ]
        )
