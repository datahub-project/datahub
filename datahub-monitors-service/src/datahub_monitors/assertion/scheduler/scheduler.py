import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from apscheduler.schedulers.background import BackgroundScheduler

from datahub_monitors.assertion.engine.engine import AssertionEngine
from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationSpec,
    CronSchedule,
)

logger = logging.getLogger(__name__)


class AssertionScheduler:
    """Class for scheduling and executing assertions based on a CRON schedule."""

    engine: AssertionEngine
    assertions: List[str]
    executor: ThreadPoolExecutor
    scheduler: BackgroundScheduler
    default_schedule: str = "0 * * * *"  # TODO: Make this configurable.
    default_timezone: str = "America/Los_Angeles"

    def __init__(
        self,
        engine: AssertionEngine,
        assertions: Optional[List[AssertionEvaluationSpec]] = None,
        default_schedule: Optional[str] = None,  # equivalent to @hourly in cron format
        default_timezone: Optional[str] = None,
    ):
        """
        Initialize the AssertionScheduler with a list of assertions and a default schedule.

        :param engine: An instance of AssertionEngine for evaluating assertions
        :param assertions: A list of assertion specs representing the assertions to be scheduled. Defaults to an empty list if not provided.
        :param default_schedule: The default CRON schedule for assertions without a specified schedule.
        :param default_timezone: The default CRON timezone for assertions without a specified schedule.
        """
        self.engine = engine if engine is not None else AssertionEngine([])
        self.assertions = (
            [assertion_spec.assertion.urn for assertion_spec in assertions]
            if assertions is not None
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
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        context: AssertionEvaluationContext,
    ) -> None:
        try:
            logger.debug(
                f"Running scheduled evaluation for assertion with urn {assertion.urn}"
            )
            self.engine.evaluate(assertion, parameters, context)
        except Exception:
            logger.exception(
                f"Failed to evaluate scheduled assertion with urn {assertion.urn}! This means that no assertion results will be produced and could indicate missing data."
            )

    def submit_assertion(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        context: AssertionEvaluationContext,
    ) -> None:
        """
        Submit an assertion to the thread pool for evaluation.
        If we need to scale the scheduler in the future, this will produce a Kafka
        event which is consumed by downstream consumers to evaluate the Assertion.

        :param assertion: The assertion to be evaluated.
        :param parameters: The parameters required to evaluate the assertion
        :param context: The context for the evaluation.
        """
        self.executor.submit(self.wrapped_evaluate, assertion, parameters, context)

    def schedule_assertion(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        schedule: CronSchedule,
        context: AssertionEvaluationContext,
    ) -> str:
        """
        Schedule an assertion based on its CRON schedule, or the default schedule if not specified.

        :param assertion: The assertion to be scheduled.
        :param parameters: The parameters required to evaluate the assertion.
        :param schedule: The schedule on which to evaluate the assertion.
        :param context: The context for the evaluation.
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
            f"Scheduling assertion evaluation job for assertion with urn {assertion.urn} at {cron}"
        )

        job = self.scheduler.add_job(
            self.submit_assertion,
            trigger="cron",
            args=[assertion, parameters, context],
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone=timezone,  # specify the timezone here
            misfire_grace_time=6 * 60 * 60,  # 6 hour misfire grace period
        )
        return job.id

    def unschedule_assertion(self, job_id: str) -> None:
        """
        Unschedule a job.

        :param job_id: The job id to be unscheduled.
        """
        self.scheduler.remove_job(job_id)

    def add_assertion(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        schedule: CronSchedule,
        context: AssertionEvaluationContext,
    ) -> str:
        """
        Add an assertion to the list and schedule it.

        :param assertion: The assertion to be scheduled.
        :param parameters: The parameters required to evaluate the assertion.
        :param schedule: The schedule on which to evaluate the assertion.
        :param context: The context for the evaluation.
        :return: The scheduled job id.
        """
        self.assertions.append(assertion.urn)
        return self.schedule_assertion(assertion, parameters, schedule, context)

    def remove_assertion(self, assertion: Assertion) -> None:
        """
        Remove an assertion from the list and unschedule it.

        :param assertion: The assertion to be removed and unscheduled.
        """
        if assertion.urn in self.assertions:
            self.assertions.remove(assertion.urn)
        for job in self.scheduler.get_jobs():
            if job.args[0].urn == assertion.urn:
                self.unschedule_assertion(job.id)
