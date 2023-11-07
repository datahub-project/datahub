import logging
from typing import Any, List

from datahub_monitors.common.types import MonitorMode
from datahub_monitors.service.scheduler.fetcher.config import FetcherConfig

logger = logging.getLogger(__name__)


def build_filters(config: FetcherConfig) -> List[Any]:
    or_filters: List[Any] = []
    active_filter = {
        "field": "mode",
        "values": [MonitorMode.ACTIVE.value, MonitorMode.PASSIVE.value],
        "condition": "EQUAL",
    }

    if config.executor_ids is not None:
        # If a specific set of executor ids was configured, then always use them
        or_filters.append(
            {
                "and": [
                    {
                        "field": "executorId",
                        "condition": "EQUALS",
                        "values": config.executor_ids,
                    },
                    active_filter,
                ]
            }
        )
    else:
        or_filters.append({"and": [active_filter]})

    return or_filters
