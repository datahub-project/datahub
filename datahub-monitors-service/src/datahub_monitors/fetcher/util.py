from typing import Any, List

from datahub_monitors.fetcher.types import MonitorFetcherConfig, MonitorFetcherMode


def build_filters(config: MonitorFetcherConfig) -> List[Any]:
    or_filters: List[Any] = []

    if config.executor_ids is not None:
        # If a specific set of executor ids was configured, then always use them
        or_filters.append(
            {
                "and": [
                    {
                        "field": "executorId",
                        "condition": "EQUALS",
                        "values": config.executor_ids,
                    }
                ]
            }
        )

    if config.mode == MonitorFetcherMode.DEFAULT:
        # If we have a default executor, always fetch the monitors that do not have an executor id
        # explicitly set.
        # Note that in the future, we may need dynamic monitor assignment to avoid
        # overloading a single node.
        or_filters.append(
            {"and": [{"field": "executorId", "condition": "EXISTS", "negated": True}]}
        )

    return or_filters
