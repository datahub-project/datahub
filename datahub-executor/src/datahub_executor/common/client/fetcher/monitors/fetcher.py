import logging
from typing import List

from tenacity import retry, stop_after_attempt, wait_exponential
from tenacity.before_sleep import before_sleep_log

from datahub_executor.common.client.fetcher.base import Fetcher
from datahub_executor.common.client.fetcher.monitors.graphql.query import (
    GRAPHQL_LIST_MONITORS_OPERATION,
    GRAPHQL_LIST_MONITORS_QUERY,
)
from datahub_executor.common.client.fetcher.monitors.mapper import (
    graphql_to_monitors,
    monitors_to_execution_requests,
)
from datahub_executor.common.client.fetcher.monitors.util import build_filters
from datahub_executor.common.constants import LIST_MONITORS_BATCH_SIZE
from datahub_executor.common.helpers import paginate_datahub_query_results
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import ExecutionRequestSchedule, Monitor

logger = logging.getLogger(__name__)


class MonitorFetcher(Fetcher):
    """Class used to fetch monitors from an external API."""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
        retry_error_callback=lambda x: METRIC(
            "ASSERTION_FETCHER_ERRORS", exception="Retry"
        ).inc(),
    )
    @METRIC("ASSERTION_FETCHER_REQUESTS").time()  # type: ignore
    def _fetch_monitors(self) -> List[Monitor]:
        """
        Fetch the list of monitors from the API.

        :return: The list of monitors.
        """
        result = paginate_datahub_query_results(
            graph=self.graph,
            query=GRAPHQL_LIST_MONITORS_QUERY,
            operation_name=GRAPHQL_LIST_MONITORS_OPERATION,
            query_key="searchAcrossEntities",
            result_key="searchResults",
            user_params={
                "input": {
                    "types": ["MONITOR"],
                    "query": "*",
                    "searchFlags": {"skipCache": True},
                    "orFilters": build_filters(self.config),
                }
            },
            page_size=LIST_MONITORS_BATCH_SIZE,
        )
        monitors = [item["entity"] for item in result]
        return graphql_to_monitors(monitors)

    def fetch_execution_requests(self) -> List[ExecutionRequestSchedule]:
        raw = self._fetch_monitors()
        monitors = monitors_to_execution_requests(raw)
        self.touch()
        return monitors
