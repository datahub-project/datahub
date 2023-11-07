import logging
from typing import Dict, List

from acryl.executor.request.execution_request import ExecutionRequest

from datahub_monitors.common.types import AssertionEvaluationContext, Monitor
from datahub_monitors.service.scheduler.monitors.util import is_dry_run_mode
from datahub_monitors.service.scheduler.types import (
    RUN_ASSERTION_TASK_NAME,
    ExecutionRequestSchedule,
)

logger = logging.getLogger(__name__)


def graphql_to_monitors(graphql_monitors: List[Dict]) -> List[Monitor]:
    logger.debug(f"Converting GraphQL monitors to Engine Monitors {graphql_monitors}")
    monitors = []
    for graphql_monitor in graphql_monitors:
        try:
            # Simply parse to our Pydantic models using the raw GraphQL Response.
            monitors.append(Monitor.parse_obj(graphql_monitor))
        except Exception:
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
        assertion_specs = (
            monitor.assertion_monitor.assertions
            if monitor.assertion_monitor is not None
            else []
        )
        dry_run = is_dry_run_mode(monitor)
        if assertion_specs:
            for assertion_spec in assertion_specs:
                context = AssertionEvaluationContext(
                    dry_run=dry_run, monitor_urn=monitor.urn
                )

                execution_request = ExecutionRequest(
                    executor_id=monitor.executor_id
                    if monitor.executor_id
                    else "default",
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

    return execution_requests
