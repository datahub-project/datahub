import logging
from typing import Dict, List

from acryl.executor.request.execution_request import ExecutionRequest

from datahub_executor.common.client.fetcher.monitors.util import (
    get_hourly_monitor_training_schedule_with_jitter,
    is_dry_run_mode,
)
from datahub_executor.common.constants import (
    DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
    RUN_ASSERTION_TASK_NAME,
    RUN_MONITOR_TRAINING_TASK_NAME,
)
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import (
    AssertionEvaluationContext,
    AssertionEvaluationSpec,
    AssertionSourceType,
    CronSchedule,
    ExecutionRequestSchedule,
    Monitor,
)
from datahub_executor.config import (
    IS_LOCAL_DEV,
    MONITOR_TRAINING_EXECUTOR_ID,
    ONLINE_SMART_ASSERTIONS_ENABLED,
)

logger = logging.getLogger(__name__)

MINUTE_SCHEDULE = "* * * * *"


class SkippableMonitorMappingError(Exception):
    pass


def graphql_to_monitor(graphql_monitor: Dict) -> Monitor:
    # Simply parse to our Pydantic models using the raw GraphQL Response.
    parsed_monitor = Monitor.model_validate(graphql_monitor)
    if parsed_monitor.assertion_monitor is None:
        raise SkippableMonitorMappingError(
            f"Skipping monitor {parsed_monitor.urn} as it does not have an assertion monitor"
        )
    if len(parsed_monitor.assertion_monitor.assertions) != 1:
        raise SkippableMonitorMappingError(
            f"Skipping monitor {parsed_monitor.urn} as it does not have exactly 1 assertion associated with it. Not yet supported!"
        )
    if parsed_monitor.assertion_monitor.assertions[0].assertion.entity.exists is False:
        raise SkippableMonitorMappingError(
            f"Skipping monitor {parsed_monitor.urn} as the associated entity is soft deleted!"
        )
    return parsed_monitor


def graphql_to_monitors(graphql_monitors: List[Dict]) -> List[Monitor]:
    logger.debug(f"Converting GraphQL monitors to Engine Monitors {graphql_monitors}")
    monitors = []
    for graphql_monitor in graphql_monitors:
        try:
            METRIC("ASSERTION_FETCHER_ITEMS_MAPPED").inc()

            monitor = graphql_to_monitor(graphql_monitor)
            monitors.append(monitor)
        except SkippableMonitorMappingError as e:
            logger.warning(e)
        except Exception as e:
            error_message = str(e)
            # Check if the specific validation error is in the exception message
            if (
                "assertion_monitor -> assertions -> 0 -> assertion -> entity"
                in error_message
            ):
                logger.error(
                    f"Validation error ignored for monitor. {graphql_monitor}: {error_message}"
                )
            else:
                METRIC("ASSERTION_FETCHER_ITEMS_ERRORED", exception="exception").inc()
                logger.exception(
                    f"Failed to convert GraphQL Monitor object to Python object. {graphql_monitor}"
                )
    logger.debug(f"Finished converting GraphQL monitors to Engine monitors {monitors}")
    return monitors


def monitors_to_execution_requests(
    monitors: List[Monitor],
) -> List[ExecutionRequestSchedule]:
    """
    Generates execution requests (tasks) for a list of monitors.
    Each monitor can have at most two tasks:
    1. Assertion evaluation
    2. Monitor training (only for inferred assertions if smart assertions are enabled)
    """
    execution_requests = []

    for monitor in monitors:
        try:
            execution_requests.extend(generate_assertion_tasks(monitor))

            if ONLINE_SMART_ASSERTIONS_ENABLED:
                logger.debug("Adding monitor training execution requests...")
                execution_requests.extend(generate_training_tasks(monitor))

        except Exception as e:
            METRIC("ASSERTION_FETCHER_ITEMS_MAPPED").inc()
            logger.warning(f"Exception while processing monitor {monitor.urn}: {e}")

    return execution_requests


def generate_assertion_tasks(monitor: Monitor) -> List[ExecutionRequestSchedule]:
    """
    Generates assertion evaluation tasks for a given monitor.
    Returns a list of ExecutionRequestSchedule objects.
    """
    execution_requests = []
    assertion_specs = (
        monitor.assertion_monitor.assertions if monitor.assertion_monitor else []
    )
    dry_run = is_dry_run_mode(monitor)

    for assertion_spec_raw in assertion_specs:
        assertion_spec = _truncate_assertion_spec_for_execution_request(
            assertion_spec_raw.copy(deep=True)
        )
        context = AssertionEvaluationContext(
            dry_run=dry_run,
            online_smart_assertions=ONLINE_SMART_ASSERTIONS_ENABLED,
            monitor_urn=monitor.urn,
            assertion_evaluation_spec=assertion_spec,
        )

        execution_request = ExecutionRequest(
            executor_id=monitor.executor_id or DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
            exec_id=f"{monitor.urn}_scheduled_training",
            name=RUN_ASSERTION_TASK_NAME,
            args={
                "urn": monitor.urn,
                "assertion_spec": assertion_spec.model_dump(by_alias=True),
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


def generate_training_tasks(raw_monitor: Monitor) -> List[ExecutionRequestSchedule]:
    """
    Generates monitor training tasks for inferred assertions.
    Returns a list of ExecutionRequestSchedule objects.
    """

    # Truncate the monitor's embedded assertions to avoid sending too much data in the execution request
    monitor = raw_monitor.copy(deep=True)
    if monitor.assertion_monitor:
        for i, assertion_spec in enumerate(monitor.assertion_monitor.assertions):
            monitor.assertion_monitor.assertions[i] = (
                _truncate_assertion_spec_for_execution_request(assertion_spec)
            )

    execution_requests = []
    assertion_specs = (
        monitor.assertion_monitor.assertions if monitor.assertion_monitor else []
    )

    for assertion_spec in assertion_specs:
        if assertion_spec.assertion.source_type == AssertionSourceType.INFERRED:
            context = AssertionEvaluationContext(
                dry_run=is_dry_run_mode(monitor),
                monitor_urn=monitor.urn,
                online_smart_assertions=ONLINE_SMART_ASSERTIONS_ENABLED,
                assertion_evaluation_spec=assertion_spec,
            )

            training_execution_request = ExecutionRequest(
                executor_id=MONITOR_TRAINING_EXECUTOR_ID,
                exec_id=monitor.urn,
                name=RUN_MONITOR_TRAINING_TASK_NAME,
                args={
                    "urn": monitor.urn,
                    "monitor": monitor.model_dump(by_alias=True),
                    "context": context.__dict__,
                },
            )

            monitor_training_schedule = (
                MINUTE_SCHEDULE
                if IS_LOCAL_DEV
                else get_hourly_monitor_training_schedule_with_jitter(monitor.urn)
            )

            logger.debug(
                f"Found an assertion requiring training: {assertion_spec.assertion.urn} schedule: {monitor_training_schedule}"
            )

            execution_requests.append(
                ExecutionRequestSchedule(
                    execution_request=training_execution_request,
                    schedule=CronSchedule(
                        cron=monitor_training_schedule,
                        timezone="UTC",
                    ),
                )
            )

    return execution_requests


def _truncate_assertion_spec_for_execution_request(
    assertion_spec: AssertionEvaluationSpec,
) -> AssertionEvaluationSpec:
    """
    Truncate the assertion spec for the execution request.
    """
    if assertion_spec.context is not None:
        assertion_spec.context.embedded_assertions = None
    if assertion_spec.assertion is not None:
        assertion_spec.assertion.monitor = None
    return assertion_spec
