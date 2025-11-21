import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests

from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
from tests.utilities import env_vars, release_test_reporter_helpers
from tests.utilities.metadata_operations import get_default_channel_name
from tests.utilities.slack_helpers import (
    get_channel_id_by_name as slack_get_channel_id_by_name,
    send_message as slack_send_message,
    update_message as slack_update_message,
)
from tests.utils import get_integrations_service_url

logger = logging.getLogger(__name__)


class NotificationSink:
    """Protocol for notification sinks that receive test progress updates."""

    def on_collection_complete(self, total_tests: int) -> None:
        """Called when test collection completes."""
        pass

    def on_progress_update(
        self, passed: int, failed: int, skipped: int, total_tests: int
    ) -> None:
        """Called when test progress should be updated."""
        pass

    def on_test_complete(
        self,
        exitstatus: int,
        passed: int,
        failed: int,
        skipped: int,
        failed_tests: set,
        message: str,
    ) -> None:
        """Called when all tests complete."""
        pass


@dataclass
class SlackConfig:
    """Slack configuration for operations."""

    token: str
    channel: str
    test_identifier: str


class SlackNotificationSink(NotificationSink):
    """Sends notifications to internal Slack channel."""

    def __init__(
        self,
        slack_api_token: str | None = None,
        slack_channel: str | None = None,
        test_identifier: str | None = None,
        slack_thread_ts: str | None = None,
    ):
        self._slack_thread_ts: str | None = slack_thread_ts
        self._main_msg_ts: str | None = None
        self.config: SlackConfig | None = None

        # Resolve channel ID and create config if we have the necessary parameters
        if slack_api_token and slack_channel:
            logger.info(f"Resolving slack channel {slack_channel} to ID")
            slack_channel_id = slack_get_channel_id_by_name(
                slack_api_token, slack_channel
            )

            if slack_channel_id:
                logger.info(
                    f"Resolved slack channel {slack_channel} to ID {slack_channel_id}"
                )
                self.config = SlackConfig(
                    token=slack_api_token,
                    channel=slack_channel_id,
                    test_identifier=test_identifier or "Test",
                )
            else:
                logger.error(f"Could not find slack channel {slack_channel}")

    def _format_multi_timezone_timestamp(self) -> str:
        """Format current time in multiple timezones."""
        now = datetime.now(ZoneInfo("UTC"))

        timezones = [
            ("IST", "Asia/Kolkata"),
            ("UTC", "UTC"),
            ("EDT", "America/New_York"),
            ("PDT", "America/Los_Angeles"),
        ]

        formatted_times = []
        for tz_abbr, tz_name in timezones:
            tz_time = now.astimezone(ZoneInfo(tz_name))
            formatted_times.append(f"{tz_time.strftime('%H:%M')} {tz_abbr}")

        return f"\nLast Updated {' | '.join(formatted_times)}"

    def on_collection_complete(self, total_tests: int) -> None:
        """Send initial message when test collection completes."""
        try:
            if self.config is None:
                return

            message = (
                f"{self.config.test_identifier} - Collected {total_tests} tests. Starting test run..."
                + self._format_multi_timezone_timestamp()
            )

            response = slack_send_message(
                token=self.config.token,
                channel=self.config.channel,
                message=message,
                thread_ts=self._slack_thread_ts,
            )

            # Capture thread timestamp if not already set
            if self._slack_thread_ts is None and response.get("ok"):
                self._slack_thread_ts = response.get("ts")
                logger.info(f"Created new Slack thread: {self._slack_thread_ts}")

            # Store the main message timestamp so we can update it later
            if response.get("ok"):
                self._main_msg_ts = response.get("ts")
                logger.info(
                    f"Initial message posted - ts: {self._main_msg_ts}, "
                    f"channel from response: {response.get('channel')}, "
                    f"channel used in request: {self.config.channel}"
                )
        except Exception:
            logger.exception("Failed to send collection message to Slack")

    def on_progress_update(
        self, passed: int, failed: int, skipped: int, total_tests: int
    ) -> None:
        """Update the main message with progress information."""
        try:
            if self.config is None:
                return

            completed = passed + failed + skipped
            progress_pct = (completed / total_tests * 100) if total_tests > 0 else 0

            message = (
                "Smoke test progress update:\n"
                f"Completed: {completed}/{total_tests} ({progress_pct:.1f}%)\n"
                f"✅ Passed: {passed}\n"
                f"❌ Failed: {failed}\n"
                f"⏭️  Skipped: {skipped}" + self._format_multi_timezone_timestamp()
            )
            logger.info(f"{message}")

            if self._main_msg_ts is None:
                logger.warning(
                    "No main message timestamp available for progress update"
                )
                return

            logger.debug(
                f"Attempting to update message - ts: {self._main_msg_ts}, "
                f"channel: {self.config.channel}"
            )

            # Update existing main message
            response = slack_update_message(
                token=self.config.token,
                channel=self.config.channel,
                ts=self._main_msg_ts,
                message=message,
            )

            logger.info(
                f"Update response - ok: {response.get('ok')}, "
                f"error: {response.get('error')}"
            )
        except Exception:
            logger.exception("Failed to send progress update to Slack")

    def on_test_complete(
        self,
        exitstatus: int,
        passed: int,
        failed: int,
        skipped: int,
        failed_tests: set,
        message: str,
    ) -> None:
        """Update the main message with final test results."""
        try:
            if self.config is None:
                return

            logger.info(f"Final status message\n{message}\n")

            if self._main_msg_ts is None:
                logger.warning("No main message timestamp available for final update")
                return

            # lets not look at exit status for reporting since that may also capture non-test failures. For ex a misconfigured internal slack
            # we still have detailed logs
            passed_str = "PASSED" if failed == 0 else "FAILED"
            full_message = (
                f"{self.config.test_identifier} Status - {passed_str}\n{message}"
                + self._format_multi_timezone_timestamp()
            )

            logger.info(
                f"Attempting to update final message - ts: {self._main_msg_ts}, "
                f"channel: {self.config.channel}"
            )

            # Update existing main message
            response = slack_update_message(
                token=self.config.token,
                channel=self.config.channel,
                ts=self._main_msg_ts,
                message=full_message,
            )

            logger.info(
                f"Final update response - ok: {response.get('ok')}, "
                f"error: {response.get('error')}"
            )
        except Exception:
            logger.exception("Failed to send final message to Slack")


class IntegrationsServiceNotificationSink(NotificationSink):
    """Sends notifications via integrations service to customer notification channels."""

    def __init__(
        self, notify_customers: bool = False, default_channel_name: str | None = None
    ):
        self.notify_customers = notify_customers
        self.default_channel_name = default_channel_name

    def on_collection_complete(self, total_tests: int) -> None:
        """No action needed on collection complete."""
        pass

    def on_progress_update(
        self, passed: int, failed: int, skipped: int, total_tests: int
    ) -> None:
        """No action needed on progress update."""
        pass

    def _send_notification(
        self, channel_name: str, message: str, status_str: str
    ) -> None:
        """Send notification via integrations service to specified Slack channel."""
        try:
            integrations_url = get_integrations_service_url()
            endpoint = f"{integrations_url}/private/notifications/send"

            notification_payload = {
                "message": {
                    "template": "RELEASE_NOTIFICATION",
                    "parameters": {
                        "title": f"DataHub Release Test Results - {status_str}",
                        "body": message,
                    },
                },
                "recipients": [
                    {
                        "type": "SLACK_CHANNEL",
                        "id": channel_name,
                    }
                ],
            }

            logger.info(
                f"Sending notification to integrations service at {endpoint} "
                f"for channel {channel_name}"
            )

            response = requests.post(
                endpoint,
                json=notification_payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            logger.info(
                f"Successfully sent notification to integrations service. "
                f"Status: {response.status_code}"
            )

        except Exception as e:
            logger.warning(
                f"Failed to send notification via integrations service to "
                f"channel {channel_name}: {e}"
            )

    def on_test_complete(
        self,
        exitstatus: int,
        passed: int,
        failed: int,
        skipped: int,
        failed_tests: set,
        message: str,
    ) -> None:
        """Send notification via integrations service if configured."""
        try:
            logger.info(
                f"IntegrationsServiceNotificationSink.on_test_complete called with "
                f"notify_customers={self.notify_customers}"
            )

            if not self.notify_customers:
                logger.info(
                    "Skipping notification via integrations service because NOTIFY_CUSTOMERS is not true"
                )
                return

            default_channel = self.default_channel_name
            if not default_channel:
                logger.info(
                    "Default channel not configured, cannot send slack notification in customer slack channel"
                )
                return

            logger.info(
                f"Sending notification via integrations service to "
                f"channel: {default_channel}"
            )

            status_str = "PASSED" if exitstatus == 0 else "FAILED"
            self._send_notification(default_channel, message, status_str)
        except Exception:
            logger.exception("Failed to send notification via integrations service")


class DataHubReportingSink(NotificationSink):
    """Reports test metadata to internal DataHub for tracking and monitoring."""

    def __init__(self):
        self._pipeline_urn: Optional[str] = None
        self._task_urn: Optional[str] = None
        self._process_instance: Optional[DataProcessInstance] = None
        self._start_timestamp_millis: Optional[int] = None

    def on_collection_complete(self, total_tests: int) -> None:
        """Initialize test tracking in DataHub."""
        try:
            (
                self._pipeline_urn,
                self._task_urn,
                self._process_instance,
                self._start_timestamp_millis,
            ) = release_test_reporter_helpers.initialize_test_tracking()
        except Exception:
            logger.exception("Failed to initialize DataHub test tracking")

    def on_progress_update(
        self, passed: int, failed: int, skipped: int, total_tests: int
    ) -> None:
        """No action needed on progress update."""
        pass

    def on_test_complete(
        self,
        exitstatus: int,
        passed: int,
        failed: int,
        skipped: int,
        failed_tests: set,
        message: str,
    ) -> None:
        """Finalize test tracking in DataHub."""
        try:
            release_test_reporter_helpers.finalize_test_tracking(
                task_urn=self._task_urn,
                pipeline_urn=self._pipeline_urn,
                process_instance=self._process_instance,
                start_timestamp_millis=self._start_timestamp_millis,
                failed_count=failed,
                passed_count=passed,
                skipped_count=skipped,
                failed_tests=failed_tests,
                message=message,
            )
        except Exception:
            logger.exception("Failed to finalize DataHub test tracking")


class TestProgressTracker:
    """Track test progress and notify configured sinks."""

    def __init__(self, auth_session, sinks: list[NotificationSink] | None = None):
        self.total_tests = 0
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.last_update_time = 0.0
        # tuples of test nodeid and User visible description of the test that failed
        self.failed_tests: set[tuple[str, str]] = set()
        self.min_update_interval = 10  # Minimum seconds between updates
        self.update_frequency = 5  # Send update every N tests
        self._counted_tests: dict[str, str] = {}  # nodeid -> outcome
        self.datahub_stats: dict[str, Any] = {}

        # Notification sinks
        self.sinks = sinks if sinks is not None else []

    def has_counted_test(self, nodeid: str) -> bool:
        """Check if a test has already been counted."""
        return nodeid in self._counted_tests

    def get_test_outcome(self, nodeid: str) -> str | None:
        """Get the recorded outcome for a test."""
        return self._counted_tests.get(nodeid)

    def record_failure(self, nodeid: str, test_desc: str | None = None) -> None:
        """Record a test failure, handling state transitions."""

        logger.info(f"Recording failure {nodeid}, {test_desc}")
        if nodeid not in self._counted_tests:
            self._counted_tests[nodeid] = "failed"
            self.failed += 1
        elif self._counted_tests[nodeid] == "passed":
            # Test passed call phase but failed in teardown
            self._counted_tests[nodeid] = "failed"
            self.passed -= 1
            self.failed += 1

        if test_desc is None:
            test_desc = TestProgressTracker.convert_to_desc(nodeid)
        self.failed_tests.add((nodeid, test_desc))
        self.send_update_if_needed()

    @staticmethod
    def convert_to_desc(nodeid):
        # sample nodeid for pytests:  tests/read_only/test_search.py::test_openapi_v3_entity
        # Since all tests added will run through this during dev, any tests that don't follow the convention will get flagged.
        return nodeid.split("::")[1].replace("_", " ")

    def record_outcome(self, nodeid: str, outcome: str) -> None:
        """Record a test outcome (passed or skipped)."""
        desc = TestProgressTracker.convert_to_desc(nodeid)

        logger.info(f"record_outcome {nodeid}, {outcome}, {desc}")

        # Cypress test nodeids may be duplicate -- cant disambiguate multiple tests in one spec.
        self._counted_tests[nodeid] = outcome

        if outcome == "passed":
            self.passed += 1
        elif outcome == "skipped":
            self.skipped += 1

        self.send_update_if_needed()

    def add_datahub_stat(self, stat_name: str, stat_val: Any) -> None:
        """Add a DataHub statistic."""
        self.datahub_stats[stat_name] = stat_val

    def send_update_if_needed(self):
        if self.should_send_update():
            logger.info("Sending progress update to notification sinks")
            self.send_update()

    def should_send_update(self, force: bool = False) -> bool:
        """Determine if we should send a progress update."""
        if force:
            return True

        completed = self.passed + self.failed + self.skipped
        time_since_last = time.time() - self.last_update_time

        # Send update every N tests or if enough time has passed
        return (
            completed % self.update_frequency == 0
            and time_since_last >= self.min_update_interval
        )

    def _build_test_results_message(self, add_datahub_stats: bool = True) -> str:
        """Build the test results message body."""
        message = ""

        # Use the test desc if available, else the test name.
        # Some formatting, indent the test desc under failed tests.
        failure_details = ""
        logger.info("failure list")
        logger.info(self.failed_tests)
        if self.failed_tests:
            failure_details = "\nFailed tests:\n" + "\n".join(
                [
                    f"  {failed_test[1] or failed_test[0]}"
                    for failed_test in self.failed_tests
                ]
            )

        message += f"\nYour Datahub instance {env_vars.get_frontend_url()} is upgraded to {env_vars.get_release_version()}\n"
        if self.failed > 0:
            message += f"❌ There were test failures\n{failure_details}\n"
        else:
            message += "✅ All tests passed\n"
        return message

    def send_collection_message(self) -> None:
        """Notify all sinks that test collection is complete."""
        for sink in self.sinks:
            try:
                sink.on_collection_complete(self.total_tests)
            except Exception:
                logger.exception(
                    f"Failed to notify sink {sink.__class__.__name__} of collection complete"
                )

    def send_progress_message(self) -> None:
        """Notify all sinks of progress update."""
        for sink in self.sinks:
            try:
                sink.on_progress_update(
                    self.passed, self.failed, self.skipped, self.total_tests
                )
            except Exception:
                logger.exception(
                    f"Failed to notify sink {sink.__class__.__name__} of progress update"
                )

    def send_final_message(self, exitstatus: int) -> None:
        """Notify all sinks that tests are complete."""
        message = self._build_test_results_message()

        for sink in self.sinks:
            try:
                sink.on_test_complete(
                    exitstatus,
                    self.passed,
                    self.failed,
                    self.skipped,
                    self.failed_tests,
                    message,
                )
            except Exception:
                logger.exception(
                    f"Failed to notify sink {sink.__class__.__name__} of test completion"
                )

    def send_update(self):
        """Send progress update to all sinks."""
        self.send_progress_message()
        self.last_update_time = time.time()


# Module-level tracker instance for backward compatibility
_module_tracker: TestProgressTracker | None = None
_module_tracker_lock = threading.Lock()


def get_module_tracker() -> TestProgressTracker:
    from conftest import (
        auth_session_context,  # Delayed loading to avoid circular imports
    )

    """Get or create the module-level tracker instance (thread-safe)."""
    global _module_tracker
    if _module_tracker is None:
        with _module_tracker_lock:
            # Double-check inside the lock to prevent race condition
            if _module_tracker is None:
                with auth_session_context() as auth_session:
                    # Initialize all notification sinks
                    slack_api_token = env_vars.get_slack_api_token()
                    slack_channel = env_vars.get_slack_channel()
                    test_identifier = env_vars.get_test_identifier()
                    slack_thread_ts = env_vars.get_slack_thread_ts()
                    notify_customers = env_vars.get_notify_customers() == "true"
                    default_channel_name = None
                    if notify_customers:
                        try:
                            default_channel_name = get_default_channel_name(
                                auth_session
                            )
                        except Exception:
                            logger.exception("Failed to get default channel name")

                    sinks: list[NotificationSink] = []

                    # Initialize SlackNotificationSink
                    try:
                        slack_sink = SlackNotificationSink(
                            slack_api_token=slack_api_token,
                            slack_channel=slack_channel,
                            test_identifier=test_identifier,
                            slack_thread_ts=slack_thread_ts,
                        )
                        sinks.append(slack_sink)
                    except Exception:
                        logger.exception("Failed to initialize SlackNotificationSink")

                    # Initialize IntegrationsServiceNotificationSink
                    try:
                        integrations_sink = IntegrationsServiceNotificationSink(
                            notify_customers=notify_customers,
                            default_channel_name=default_channel_name,
                        )
                        sinks.append(integrations_sink)
                    except Exception:
                        logger.exception(
                            "Failed to initialize IntegrationsServiceNotificationSink"
                        )

                    # Initialize DataHubReportingSink
                    try:
                        datahub_sink = DataHubReportingSink()
                        sinks.append(datahub_sink)
                    except Exception:
                        logger.exception("Failed to initialize DataHubReportingSink")

                    _module_tracker = TestProgressTracker(auth_session, sinks=sinks)
    return _module_tracker


def add_datahub_stats(stat_name: str, stat_val: Any) -> None:
    """Add DataHub stats - delegates to module tracker for backward compatibility."""
    get_module_tracker().add_datahub_stat(stat_name, stat_val)


def send_collection_message(total_tests: int) -> None:
    """Send collection message - delegates to module tracker for backward compatibility."""
    tracker = get_module_tracker()
    tracker.total_tests = total_tests
    tracker.send_collection_message()


def send_message(exitstatus: int, test_counts: dict | None = None) -> None:
    """Send final message - delegates to module tracker for backward compatibility."""
    get_module_tracker().send_final_message(exitstatus)
