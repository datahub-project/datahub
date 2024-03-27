import logging
import time

from typing import List

from tenacity import retry, stop_after_attempt, wait_exponential
from tenacity.before_sleep import before_sleep_log

from datahub_executor.common.client.fetcher.base import Fetcher
from datahub_executor.common.client.fetcher.monitors.graphql.query import (
    GRAPHQL_LIST_MONITORS_QUERY,
)
from datahub_executor.common.client.fetcher.monitors.mapper import (
    graphql_to_monitors,
    monitors_to_execution_requests,
)
from datahub_executor.common.client.fetcher.monitors.util import build_filters
from datahub_executor.common.constants import LIST_MONITORS_BATCH_SIZE
from datahub_executor.common.types import ExecutionRequestSchedule, Monitor

logger = logging.getLogger(__name__)


class MonitorFetcher(Fetcher):
    """Class used to fetch monitors from an external API."""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def _fetch_monitors(self) -> List[Monitor]:
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
            raise RuntimeError("GMS error")

        if (
            "searchAcrossEntities" not in result
            or "searchResults" not in result["searchAcrossEntities"]
        ):
            logger.error(
                "Found incomplete search results when fetching monitors from GMS!"
            )
            raise RuntimeError("Parse error")

        return graphql_to_monitors(
            [
                entity["entity"]
                for entity in result["searchAcrossEntities"]["searchResults"]
            ]
        )

    def fetch_execution_requests(self) -> List[ExecutionRequestSchedule]:
        raw = self._fetch_monitors()
        monitors = monitors_to_execution_requests(raw)
        self.touch()
        return monitors
