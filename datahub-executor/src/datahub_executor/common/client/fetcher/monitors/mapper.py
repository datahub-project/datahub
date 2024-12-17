import logging
from typing import Dict, List

from acryl.executor.request.execution_request import ExecutionRequest

from datahub_executor.common.client.fetcher.monitors.util import is_dry_run_mode
from datahub_executor.common.constants import RUN_ASSERTION_TASK_NAME
from datahub_executor.common.monitoring.metrics import (
    STATS_ASSERTION_FETCHER_ITEMS_ERRORED,
    STATS_ASSERTION_FETCHER_ITEMS_MAPPED,
)
from datahub_executor.common.types import (
    AssertionEvaluationContext,
    ExecutionRequestSchedule,
    Monitor,
)

logger = logging.getLogger(__name__)


def graphql_to_monitors(graphql_monitors: List[Dict]) -> List[Monitor]:
    logger.debug(f"Converting GraphQL monitors to Engine Monitors {graphql_monitors}")
    monitors = []
    for graphql_monitor in graphql_monitors:
        try:
            STATS_ASSERTION_FETCHER_ITEMS_MAPPED.inc()
            # Simply parse to our Pydantic models using the raw GraphQL Response.
            parsed_monitor = Monitor.parse_obj(graphql_monitor)
            if parsed_monitor.assertion_monitor is None:
                logger.warning(
                    f"Skipping monitor {parsed_monitor.urn} as it does not have an assertion monitor"
                )
                continue
            if len(parsed_monitor.assertion_monitor.assertions) != 1:
                logger.warning(
                    f"Skipping monitor {parsed_monitor.urn} as it does not have exactly 1 assertion associated with it. Not yet supported!"
                )
                continue
            if (
                parsed_monitor.assertion_monitor.assertions[0].assertion.entity.exists
                is False
            ):
                logger.debug(
                    f"Skipping monitor {parsed_monitor.urn} as the associated entity is soft deleted!"
                )
                continue
            monitors.append(Monitor.parse_obj(graphql_monitor))
        except Exception as e:
            error_message = str(e)
            # Check if the specific validation error is in the exception message
            if (
                "assertion_monitor -> assertions -> 0 -> assertion -> entity"
                in error_message
            ):
                logger.debug(
                    f"Validation error ignored for monitor. {graphql_monitor}: {error_message}"
                )
            else:
                STATS_ASSERTION_FETCHER_ITEMS_ERRORED.labels("exception").inc()
                logger.exception(
                    f"Failed to convert GraphQL Monitor object to Python object. {graphql_monitor}"
                )
    logger.debug(f"Finished converting GraphQL monitors to Engine monitors {monitors}")
    return monitors


def monitors_to_execution_requests(
    monitors: List[Monitor],
) -> List[ExecutionRequestSchedule]:
    execution_requests = []
    for monitor in monitors:
        try:
            assertion_specs = (
                monitor.assertion_monitor.assertions
                if monitor.assertion_monitor is not None
                else []
            )
            dry_run = is_dry_run_mode(monitor)
            if assertion_specs:
                for assertion_spec in assertion_specs:
                    context = AssertionEvaluationContext(
                        dry_run=dry_run,
                        monitor_urn=monitor.urn,
                        assertion_evaluation_spec=assertion_spec,
                    )

                    execution_request = ExecutionRequest(
                        executor_id=(
                            monitor.executor_id if monitor.executor_id else "default"
                        ),
                        exec_id=monitor.urn,
                        name=RUN_ASSERTION_TASK_NAME,
                        args={
                            "urn": monitor.urn,
                            "assertion_spec": assertion_spec.dict(by_alias=True),
                            "context": context.__dict__,
                        },
                    )

                    execution_requests.append(
                        ExecutionRequestSchedule(
                            execution_request=execution_request,
                            schedule=assertion_spec.schedule,
                        )
                    )
        except Exception as e:
            STATS_ASSERTION_FETCHER_ITEMS_MAPPED.inc()
            logger.warning(f"Exception while fetching monitors: {e}")

    return execution_requests
