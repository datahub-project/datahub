from typing import Any, List

from datahub_executor.common.types import FetcherConfig, Monitor, MonitorMode


def is_dry_run_mode(monitor: Monitor) -> bool:
    """
    If a monitor is operating in passive we always use dry_run.
    """
    status = getattr(monitor, "status", None)
    if status is not None:
        mode = getattr(status, "mode", None)
        return mode == MonitorMode.PASSIVE
    return False


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
