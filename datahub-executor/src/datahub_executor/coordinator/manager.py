import logging
from typing import Dict, List

from acryl.executor.request.execution_request import ExecutionRequest
from apscheduler.schedulers.background import BackgroundScheduler

from datahub_executor.common.client.fetcher.base import Fetcher
from datahub_executor.common.types import ExecutionRequestSchedule
from datahub_executor.config import DATAHUB_EXECUTOR_SWEEPER_INTERVAL
from datahub_executor.coordinator.scheduler import ExecutionRequestScheduler
from datahub_executor.coordinator.sweeper import SweeperJob

logger = logging.getLogger(__name__)


class ExecutionRequestManager:
    """Class for managing fetching and evaluation of execution requests."""

    def __init__(
        self,
        fetchers: List[Fetcher],
        scheduler: ExecutionRequestScheduler,
    ):
        """
        Initialize the MonitorManager with a set of Fetchers and an ExecutionRequestScheduler

        :param fetchers: A list of fetchers for fetching execution requests.
        :param scheduler: The ExecutionRequestScheduler for scheduling assertions.
        """
        self.fetchers = {}
        for fetcher in fetchers:
            self.fetchers[fetcher.config.id] = fetcher

        self.scheduler = scheduler

        self.scheduled_execution_requests: Dict[str, Dict[str, ExecutionRequest]] = {}
        self.prev_scheduled_execution_requests: Dict[
            str, Dict[str, ExecutionRequest]
        ] = {}

        # Create a background scheduler
        self.bg_scheduler = BackgroundScheduler()

        # Create and schedule sweeper
        self.sweeper = SweeperJob()
        self.bg_scheduler.add_job(
            self.sweeper.run,
            args=[],
            trigger="interval",
            seconds=DATAHUB_EXECUTOR_SWEEPER_INTERVAL,
        )

        # Schedule the refresh method to run.
        for fetcher in fetchers:
            if fetcher.config.enabled is False:
                continue

            self.scheduled_execution_requests[fetcher.config.id] = {}
            self.prev_scheduled_execution_requests[fetcher.config.id] = {}

            # run the refresh now on startup so we don't wait the entire cycle
            self.refresh_execution_requests(fetcher.config.id)

            # then also schedule the job for the next run.
            self.bg_scheduler.add_job(
                self.refresh_execution_requests,
                args=[fetcher.config.id],
                trigger="interval",
                minutes=fetcher.config.refresh_interval,
            )

    def shutdown(self) -> None:
        self.bg_scheduler.shutdown()

    def alive(self) -> bool:
        for fetcher in self.fetchers.values():
            if not fetcher.alive():
                return False
        return True

    def schedule_execution_request(
        self,
        fetcher_id: str,
        execution_request_schedule: ExecutionRequestSchedule,
    ) -> None:
        try:
            self.scheduled_execution_requests[fetcher_id][
                execution_request_schedule.execution_request.exec_id
            ] = execution_request_schedule.execution_request
            self.scheduler.remove_execution_request(
                execution_request_schedule.execution_request
            )
            self.scheduler.add_execution_request(
                execution_request_schedule.execution_request,
                execution_request_schedule.schedule,
            )
        except Exception as e:
            logger.error(f"Scheduler: failed to add/update execution request: {e}")

    def unschedule_deleted_execution_requests(self, fetcher_id: str) -> None:
        scheduled_urns = [
            execution_request.exec_id
            for urn, execution_request in self.scheduled_execution_requests[
                fetcher_id
            ].items()
        ]
        for urn, execution_request in self.prev_scheduled_execution_requests[
            fetcher_id
        ].items():
            if urn in scheduled_urns:
                continue
            self.scheduler.remove_execution_request(execution_request)

    def refresh_execution_requests(self, fetcher_id: str) -> None:
        """
        Refresh the list of execution_requests by fetching them from the API
        """

        try:
            fetcher = self.fetchers[fetcher_id]

            logger.info(
                f"Attempting to refresh the set of execution requests for {fetcher_id}..."
            )
            execution_requests = fetcher.fetch_execution_requests()

            self.prev_scheduled_execution_requests[
                fetcher_id
            ] = self.scheduled_execution_requests[fetcher_id].copy()
            self.scheduled_execution_requests[fetcher_id] = {}

            for execution_request in execution_requests:
                self.schedule_execution_request(fetcher_id, execution_request)

            self.unschedule_deleted_execution_requests(fetcher_id)

            logger.info(
                f"Scheduled {len(self.scheduled_execution_requests[fetcher_id])} '{fetcher.config.id}' execution_requests!"
            )
        except Exception as e:
            logger.warning(f"Exception while refreshing execution requests: {e}")

    def start(self) -> None:
        # Start the refresh scheduler
        self.bg_scheduler.start()
