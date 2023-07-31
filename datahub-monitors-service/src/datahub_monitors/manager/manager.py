import logging
from typing import Dict

from apscheduler.schedulers.background import BackgroundScheduler

from datahub_monitors.assertion.engine.engine import AssertionEngine
from datahub_monitors.assertion.scheduler.scheduler import AssertionScheduler
from datahub_monitors.constants import LIST_MONITORS_REFRESH_INTERVAL_MINUTES
from datahub_monitors.fetcher.fetcher import MonitorFetcher
from datahub_monitors.types import (
    AssertionEvaluationContext,
    AssertionEvaluationSpec,
    Monitor,
    MonitorMode,
    MonitorType,
)

logger = logging.getLogger(__name__)


class MonitorManager:
    """Class for managing monitor fetching and evaluation."""

    def __init__(
        self,
        fetcher: MonitorFetcher,
        scheduler: AssertionScheduler,
        engine: AssertionEngine,
    ):
        """
        Initialize the MonitorManager with a MonitorFetcher, AssertionScheduler, and AssertionEngine.

        :param fetcher: The MonitorFetcher for fetching monitors.
        :param scheduler: The AssertionScheduler for scheduling assertions.
        :param engine: The AssertionEngine for evaluating assertions.
        """
        self.fetcher = fetcher
        self.scheduler = scheduler
        self.engine = engine

        self.scheduled_monitors: Dict[str, Monitor] = {}
        self.prev_scheduled_monitors: Dict[str, Monitor] = {}

        # Create a background scheduler
        self.bg_scheduler = BackgroundScheduler()
        # Schedule the refresh monitors method to run every 1 minutes
        # TODO: Make this configurable.
        self.bg_scheduler.add_job(
            self.refresh_monitors,
            "interval",
            minutes=LIST_MONITORS_REFRESH_INTERVAL_MINUTES,
        )

    def schedule_assertion_evaluation(
        self,
        assertion_spec: AssertionEvaluationSpec,
        context: AssertionEvaluationContext,
        monitor_mode: MonitorMode,
    ) -> None:
        assertion = assertion_spec.assertion
        parameters = assertion_spec.parameters
        schedule = assertion_spec.schedule
        self.scheduler.remove_assertion(assertion)
        if monitor_mode == MonitorMode.ACTIVE:
            self.scheduler.add_assertion(
                assertion,
                parameters,
                schedule,
                context,
            )

    def update_assertions_monitor(self, monitor: Monitor) -> None:
        context = AssertionEvaluationContext(monitor_urn=monitor.urn)
        assertion_specs = (
            monitor.assertion_monitor.assertions
            if monitor.assertion_monitor is not None
            else []
        )
        if assertion_specs:
            self.scheduled_monitors[monitor.urn] = monitor
            for assertion_spec in assertion_specs:
                self.schedule_assertion_evaluation(
                    assertion_spec, context, monitor.mode
                )

    # TODO: implement a proper "monitor" class that takes a definition and
    # knows how to start the monitor, instead of starting it here.
    def update_monitor(self, monitor: Monitor) -> None:
        if monitor.type == MonitorType.ASSERTION:
            self.update_assertions_monitor(monitor)
        else:
            logger.error(
                f"Unsupported Monitor type {monitor.type} provided. Skipping starting {monitor.urn}.."
            )

    def unschedule_deleted_monitors(self) -> None:
        for urn, monitor in self.prev_scheduled_monitors.items():
            if urn in self.scheduled_monitors:
                continue
            if monitor.assertion_monitor:
                for assertion_spec in monitor.assertion_monitor.assertions:
                    self.scheduler.remove_assertion(assertion_spec.assertion)

    def refresh_monitors(self) -> None:
        """
        Refresh the list of monitors by fetching them from the API
        """
        logger.info("Attempting to refresh the set of monitors...")
        monitors = self.fetcher.fetch_monitors()
        logger.info("Successfully refreshed monitors... Starting...")

        self.prev_scheduled_monitors = self.scheduled_monitors.copy()
        self.scheduled_monitors = {}

        for monitor in monitors:
            self.update_monitor(monitor)

        self.unschedule_deleted_monitors()

        logger.info("Started new monitors!")

    def start(self) -> None:
        # Start the monitor refresh scheduler
        self.bg_scheduler.start()
