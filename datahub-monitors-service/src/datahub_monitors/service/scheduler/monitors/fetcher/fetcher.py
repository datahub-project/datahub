import logging
from typing import List

from tenacity import retry, stop_after_attempt, wait_exponential
from tenacity.before_sleep import before_sleep_log

from datahub_monitors.common.constants import LIST_MONITORS_BATCH_SIZE
from datahub_monitors.common.types import Monitor
from datahub_monitors.service.scheduler.fetcher.fetcher import Fetcher
from datahub_monitors.service.scheduler.monitors.fetcher.mapper import (
    graphql_to_monitors,
    monitors_to_execution_requests,
)
from datahub_monitors.service.scheduler.monitors.fetcher.util import build_filters
from datahub_monitors.service.scheduler.monitors.graphql.query import (
    GRAPHQL_LIST_MONITORS_QUERY,
)
from datahub_monitors.service.scheduler.types import ExecutionRequestSchedule

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

    def fetch_execution_requests(self) -> List[ExecutionRequestSchedule]:
        monitors = self._fetch_monitors()
        return monitors_to_execution_requests(monitors)
