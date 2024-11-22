import logging
from typing import List, Optional

from acryl.executor.request.execution_request import ExecutionRequest
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler

from datahub_executor.common.assertion.executor import AssertionExecutor
from datahub_executor.common.constants import RUN_INGEST_TASK_NAME
from datahub_executor.common.ingestion.helpers import (
    build_execution_request_input_from_request,
    emit_execution_request_input,
)
from datahub_executor.common.monitoring.metrics import (
    STATS_SCHEDULER_ASSERTION_REQUESTS,
    STATS_SCHEDULER_INGESTION_REQUESTS,
    STATS_SCHEDULER_SUBMISSION_ERRORS,
)
from datahub_executor.common.types import CronSchedule
from datahub_executor.config import (
    DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
    DATAHUB_EXECUTOR_SCHEDULER_ASSERTIONS_MAX_THREADS,
    DATAHUB_EXECUTOR_SCHEDULER_INGESTIONS_MAX_THREADS,
    DATAHUB_EXECUTOR_SCHEDULER_MISFIRE_PERIOD,
    DATAHUB_EXECUTOR_WORKER_ID,
)
from datahub_executor.worker.remote import apply_remote_assertion_request

logger = logging.getLogger(__name__)


class ExecutionRequestScheduler:
    """Class for scheduling and executing execution_request_ids based on a CRON schedule."""

    execution_request_ids: List[str]

    ingestion_scheduler: BackgroundScheduler

    assertion_scheduler: BackgroundScheduler
    assertion_executor: AssertionExecutor

    default_schedule: str = "0 * * * *"  # TODO: Make this configurable.
    default_timezone: str = "America/Los_Angeles"

    def __init__(
        self,
        execution_requests: Optional[List[ExecutionRequest]] = None,
        default_schedule: Optional[str] = None,  # equivalent to @hourly in cron format
        default_timezone: Optional[str] = None,
        override_assertion_executor: Optional[AssertionExecutor] = None,
    ):
        """
        Initialize the ExecutionRequestScheduler with a list of execution_request_ids and a default schedule.

        :param execution_request_ids: A list of execution_request specs representing the execution_request_ids to be scheduled. Defaults to an empty list if not provided.
        :param default_schedule: The default CRON schedule for execution_request_ids without a specified schedule.
        :param default_timezone: The default CRON timezone for execution_request_ids without a specified schedule.
        """
        self.execution_request_ids = (
            [execution_request.exec_id for execution_request in execution_requests]
            if execution_requests is not None
            else []
        )

        # Assertions and Ingestions need separate schedulers. When the scheduler is shared, and either
        # assertion executor or ingestion executor thread pool is busy, the shared scheduler will block
        # completely, thus blocking both of its users.
        aeconfig = {
            "default": ThreadPoolExecutor(
                DATAHUB_EXECUTOR_SCHEDULER_ASSERTIONS_MAX_THREADS
            ),
        }
        ieconfig = {
            "default": ThreadPoolExecutor(
                DATAHUB_EXECUTOR_SCHEDULER_INGESTIONS_MAX_THREADS
            ),
        }
        self.assertion_scheduler = BackgroundScheduler(executors=aeconfig)
        self.ingestion_scheduler = BackgroundScheduler(executors=ieconfig)

        if override_assertion_executor is not None:
            self.assertion_executor = override_assertion_executor
        else:
            self.assertion_executor = AssertionExecutor()

        self.assertion_scheduler.start()
        self.ingestion_scheduler.start()

        if default_schedule is not None:
            self.default_schedule = default_schedule
        if default_timezone is not None:
            self.default_timezone = default_timezone

    def shutdown(self) -> None:
        self.assertion_scheduler.shutdown()
        self.ingestion_scheduler.shutdown()
        self.assertion_executor.shutdown()

    def submit_execution_request(
        self,
        execution_request: ExecutionRequest,
    ) -> None:
        try:
            if execution_request.exec_id == "urn:li:assertion:test":
                logger.debug("test assertion")
                return

            logger.debug(
                f"Running scheduled evaluation for execution_request with exec_id {execution_request.exec_id}"
            )
            if execution_request.name == RUN_INGEST_TASK_NAME:
                STATS_SCHEDULER_INGESTION_REQUESTS.labels(
                    execution_request.executor_id
                ).inc()
                # for scheduled ingestion, we create an ExecutionRequestInput
                # this will wind up being acted on as a pipeline (kafka) action.
                input = build_execution_request_input_from_request(execution_request)
                emit_execution_request_input(input)
            else:
                if self.should_execute_embedded(execution_request):
                    STATS_SCHEDULER_ASSERTION_REQUESTS.labels(
                        execution_request.executor_id, "true"
                    ).inc()
                    # submit request to the thread pool for async execution
                    self.assertion_executor.execute(execution_request)
                else:
                    STATS_SCHEDULER_ASSERTION_REQUESTS.labels(
                        execution_request.executor_id, "false"
                    ).inc()

                    task = apply_remote_assertion_request(
                        execution_request, execution_request.executor_id
                    )

                    logger.debug(f"started task assertion_request for exec_id = {task}")
        except Exception:
            STATS_SCHEDULER_SUBMISSION_ERRORS.labels(
                execution_request.executor_id,
                str(self.should_execute_embedded(execution_request)),
                "exception",
            ).inc()
            logger.exception(
                f"Failed to evaluate scheduled execution_request with exec_id {execution_request.exec_id}! This means that no execution_request results will be produced and could indicate missing data."
            )

    def schedule_execution_request(
        self,
        execution_request: ExecutionRequest,
        schedule: CronSchedule,
    ) -> str:
        """
        Schedule an execution_request based on its CRON schedule, or the default schedule if not specified.

        :param execution_request: The execution_request to be scheduled.
        :param schedule: The schedule on which to evaluate the execution_request.
        :return: The scheduled job id.
        """

        try:
            cron = (
                schedule.cron
                if schedule is not None and schedule.cron is not None
                else self.default_schedule
            )
            timezone = (
                schedule.timezone
                if schedule is not None and schedule.timezone is not None
                else self.default_timezone
            )

            # Parse the cron string into separate fields
            minute, hour, day, month, day_of_week = cron.split(" ")

            logger.debug(
                f"Scheduling execution_request evaluation job for execution_request with exec_id {execution_request.exec_id} at {cron}"
            )

            job = self.get_scheduler_for_execution_request(execution_request).add_job(
                self.submit_execution_request,
                trigger="cron",
                args=[execution_request],
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=timezone,
                misfire_grace_time=DATAHUB_EXECUTOR_SCHEDULER_MISFIRE_PERIOD,
            )
        except Exception as e:
            logger.warning(f"Exception while creating a scheduler job: {e}")
        return job.id

    def unschedule_execution_request(
        self, scheduler: BackgroundScheduler, job_id: str
    ) -> None:
        """
        Unschedule a job.

        :param job_id: The job id to be unscheduled.
        """
        try:
            scheduler.remove_job(job_id)
        except Exception as e:
            logger.warning(f"Exception while removing a scheduler job: {e}")

    def add_execution_request(
        self,
        execution_request: ExecutionRequest,
        schedule: CronSchedule,
    ) -> str:
        """
        Add an execution_request to the list and schedule it.

        :param execution_request: The execution_request to be scheduled.
        :param schedule: The schedule on which to evaluate the execution_request.
        :return: The scheduled job id.
        """
        if execution_request.exec_id not in self.execution_request_ids:
            self.execution_request_ids.append(execution_request.exec_id)
        return self.schedule_execution_request(execution_request, schedule)

    def remove_execution_request(self, execution_request: ExecutionRequest) -> None:
        """
        Remove an execution_request from the list and unschedule it.

        :param execution_request: The execution_request to be removed and unscheduled.
        """
        if execution_request.exec_id in self.execution_request_ids:
            self.execution_request_ids.remove(execution_request.exec_id)

        scheduler = self.get_scheduler_for_execution_request(execution_request)
        for job in scheduler.get_jobs():
            if job.args[0].exec_id == execution_request.exec_id:
                self.unschedule_execution_request(scheduler, job.id)

    def get_scheduler_for_execution_request(
        self, execution_request: ExecutionRequest
    ) -> BackgroundScheduler:
        if execution_request.name == RUN_INGEST_TASK_NAME:
            return self.ingestion_scheduler
        return self.assertion_scheduler

    def should_execute_embedded(self, execution_request: ExecutionRequest) -> bool:
        """
        Check if the execution request should be executed in the embedded worker.

        :param execution_request: The execution request to be checked.
        :return: True if the execution request should be executed in the embedded worker, False otherwise.
        """
        return DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED and (
            execution_request.executor_id is None
            or DATAHUB_EXECUTOR_WORKER_ID == execution_request.executor_id
        )
