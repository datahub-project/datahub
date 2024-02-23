import logging
from typing import List

from tenacity import retry, stop_after_attempt, wait_exponential

from datahub_monitors.common.constants import LIST_INGESTION_SOURCES_BATCH_SIZE
from datahub_monitors.service.scheduler.fetcher.fetcher import Fetcher
from datahub_monitors.service.scheduler.ingestion.fetcher.mapper import (
    graphql_to_ingestion_sources,
    ingestion_sources_to_execution_requests,
)
from datahub_monitors.service.scheduler.ingestion.graphql.query import (
    GRAPHQL_LIST_INGESTION_SOURCES_QUERY,
)
from datahub_monitors.service.scheduler.ingestion.types import IngestionSource
from datahub_monitors.service.scheduler.types import ExecutionRequestSchedule

logger = logging.getLogger(__name__)


class IngestionFetcher(Fetcher):
    """Class used to fetch ingestion sources from an external API."""

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=10)
    )
    def _fetch_ingestion_sources(self) -> List[IngestionSource]:
        """
        Fetch the list of monitors from the API.

        :return: The list of monitors.
        """
        result = self.graph.execute_graphql(
            GRAPHQL_LIST_INGESTION_SOURCES_QUERY,
            variables={
                "input": {
                    "start": 0,
                    "count": LIST_INGESTION_SOURCES_BATCH_SIZE,
                }
            },
        )

        if "error" in result and result["error"] is not None:
            # TODO: add either logging or throwing here.
            logger.error(
                f"Received error while fetching ingestion sources from GMS! {result.get('error')}"
            )
            return []

        if (
            "listIngestionSources" not in result
            or "ingestionSources" not in result["listIngestionSources"]
        ):
            logger.error(
                "Found incomplete search results when fetching ingestion sources from GMS!"
            )
            return []

        return graphql_to_ingestion_sources(
            [
                ingestion_source
                for ingestion_source in result["listIngestionSources"][
                    "ingestionSources"
                ]
            ]
        )

    def fetch_execution_requests(self) -> List[ExecutionRequestSchedule]:
        ingestion_sources = self._fetch_ingestion_sources()
        return ingestion_sources_to_execution_requests(ingestion_sources)
