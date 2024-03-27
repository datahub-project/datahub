import logging
from typing import List

from tenacity import retry, stop_after_attempt, wait_exponential

from datahub_executor.common.client.fetcher.base import Fetcher
from datahub_executor.common.client.fetcher.ingestion.graphql.query import (
    GRAPHQL_LIST_INGESTION_SOURCES_QUERY,
)
from datahub_executor.common.client.fetcher.ingestion.mapper import (
    graphql_to_ingestion_sources,
    ingestion_sources_to_execution_requests,
)
from datahub_executor.common.client.fetcher.ingestion.types import IngestionSource
from datahub_executor.common.constants import LIST_INGESTION_SOURCES_BATCH_SIZE
from datahub_executor.common.types import ExecutionRequestSchedule

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
            raise RuntimeError("GMS error")

        if (
            "listIngestionSources" not in result
            or "ingestionSources" not in result["listIngestionSources"]
        ):
            logger.error(
                "Found incomplete search results when fetching ingestion sources from GMS!"
            )
            raise RuntimeError("Parse error")

        return graphql_to_ingestion_sources(
            [
                ingestion_source
                for ingestion_source in result["listIngestionSources"][
                    "ingestionSources"
                ]
            ]
        )

    def fetch_execution_requests(self) -> List[ExecutionRequestSchedule]:
        raw = self._fetch_ingestion_sources()
        ingestions = ingestion_sources_to_execution_requests(raw)
        self.touch()
        return ingestions
