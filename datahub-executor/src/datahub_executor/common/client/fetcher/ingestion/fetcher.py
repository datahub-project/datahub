import logging
from typing import List

from tenacity import retry, stop_after_attempt, wait_exponential
from tenacity.before_sleep import before_sleep_log

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
from datahub_executor.common.helpers import paginate_datahub_query_results
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import ExecutionRequestSchedule

logger = logging.getLogger(__name__)


class IngestionFetcher(Fetcher):
    """Class used to fetch ingestion sources from an external API."""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
        retry_error_callback=lambda x: METRIC(
            "INGESTION_FETCHER_ERRORS", exception="Retry"
        ).inc(),
    )
    @METRIC("INGESTION_FETCHER_REQUESTS").time()  # type: ignore
    def _fetch_ingestion_sources(self) -> List[IngestionSource]:
        """
        Fetch the list of monitors from the API.

        :return: The list of monitors.
        """
        result = paginate_datahub_query_results(
            graph=self.graph,
            query=GRAPHQL_LIST_INGESTION_SOURCES_QUERY,
            query_key="listIngestionSources",
            result_key="ingestionSources",
            page_size=LIST_INGESTION_SOURCES_BATCH_SIZE,
            user_params={},
        )
        return graphql_to_ingestion_sources(result)

    def _fetch_default_cli_version(self) -> str:
        """
        Fetch the default CLI version from the API.
        """

        server_config = self.graph.get_server_config()

        # We don't have any fallbacks here - if we can't figure out the version to run with,
        # we should fail loudly.
        return server_config["managedIngestion"]["defaultCliVersion"]

    def fetch_execution_requests(self) -> List[ExecutionRequestSchedule]:
        raw = self._fetch_ingestion_sources()
        default_cli_version = self._fetch_default_cli_version()
        ingestions = ingestion_sources_to_execution_requests(
            raw, default_cli_version=default_cli_version
        )
        self.touch()
        return ingestions
