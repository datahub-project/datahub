import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from acryl.executor.request.execution_request import ExecutionRequest
from apscheduler.schedulers.background import BackgroundScheduler

from datahub_monitors.common.types import CronSchedule
from datahub_monitors.config import EMBEDDED_WORKER_ENABLED
from datahub_monitors.service.scheduler.types import RUN_INGEST_TASK_NAME
from datahub_monitors.service.scheduler.utils import emit_execution_request_input
from datahub_monitors.workers.assertion_executor import AssertionExecutor
from datahub_monitors.workers.helpers import update_celery_credentials
from datahub_monitors.workers.tasks import app, evaluate_execution_request

logger = logging.getLogger(__name__)


class ExecutionRequestScheduler:
    """Class for scheduling and executing execution_request_ids based on a CRON schedule."""

    execution_request_ids: List[str]
    executor: ThreadPoolExecutor
    scheduler: BackgroundScheduler
    default_schedule: str = "0 * * * *"  # TODO: Make this configurable.
    default_timezone: str = "America/Los_Angeles"

    def __init__(
        self,
        execution_requests: Optional[List[ExecutionRequest]] = None,
        default_schedule: Optional[str] = None,  # equivalent to @hourly in cron format
        default_timezone: Optional[str] = None,
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
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        if default_schedule is not None:
            self.default_schedule = default_schedule
        if default_timezone is not None:
            self.default_timezone = default_timezone

    def wrapped_evaluate(
        self,
        execution_request: ExecutionRequest,
    ) -> None:
        try:
            logger.debug(
                f"Running scheduled evaluation for execution_request with exec_id {execution_request.exec_id}"
            )
            if execution_request.name == RUN_INGEST_TASK_NAME:
                # for scheduled ingestion, we create an ExecutionRequestInput
                # this will wind up being acted on as a pipeline (kafka) action.
                emit_execution_request_input(execution_request)
            else:
                if EMBEDDED_WORKER_ENABLED:
                    thread = threading.Thread(
                        target=self._evaluate_execution_request,
                        args=(execution_request,),
                    )
                    thread.start()
                    logger.info(
                        "started task _evaluate_execution_request on a local thread"
                    )
                else:
                    # before we try to send a task over celery, we make sure we have valid SQS creds
                    update_celery_credentials(app, False, execution_request.executor_id)

                    # for others (monitors/assertions) we directly trigger the task run.
                    task = evaluate_execution_request.apply_async(
                        args=[execution_request],
                        task_id=execution_request.args["urn"],
                        queue=execution_request.executor_id,
                        routing_key=f"{execution_request.executor_id}.evaluate_execution_request",
                    )
                    logger.debug(
                        f"started task evaluate_execution_request task_id = {task.id}"
                    )
        except Exception:
            logger.exception(
                f"Failed to evaluate scheduled execution_request with exec_id {execution_request.exec_id}! This means that no execution_request results will be produced and could indicate missing data."
            )

    def _evaluate_execution_request(self, execution_request: ExecutionRequest) -> None:
        assertion_executor = AssertionExecutor()
        assertion_executor.execute(execution_request)

    def submit_execution_request(
        self,
        execution_request: ExecutionRequest,
    ) -> None:
        """
        Submit an execution_request to the thread pool for evaluation.

        :param execution_request: The execution_request to be evaluated.
        """
        self.executor.submit(self.wrapped_evaluate, execution_request)

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

        job = self.scheduler.add_job(
            self.submit_execution_request,
            trigger="cron",
            args=[execution_request],
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone=timezone,  # specify the timezone here
            misfire_grace_time=6 * 60 * 60,  # 6 hour misfire grace period
        )
        return job.id

    def unschedule_execution_request(self, job_id: str) -> None:
        """
        Unschedule a job.

        :param job_id: The job id to be unscheduled.
        """
        self.scheduler.remove_job(job_id)

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
        for job in self.scheduler.get_jobs():
            if job.args[0].exec_id == execution_request.exec_id:
                self.unschedule_execution_request(job.id)
