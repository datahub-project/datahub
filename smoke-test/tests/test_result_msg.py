import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import requests
from slack_sdk.web import SlackResponse

from tests.utilities import env_vars
from tests.utilities.metadata_operations import get_default_channel_name
from tests.utilities.slack_helpers import (
    get_channel_id_by_name as slack_get_channel_id_by_name,
    send_message as slack_send_message,
    update_message as slack_update_message,
)
from tests.utils import get_integrations_service_url

logger = logging.getLogger(__name__)


@dataclass
class SlackConfig:
    """Slack configuration from environment variables."""

    token: str
    channel: str
    test_identifier: str


@contextmanager
def slack_operation(operation_name: str) -> Iterator[SlackConfig | None]:
    """
    Context manager for Slack operations.

    Handles config retrieval, validation, and error logging.
    Yields None if Slack is not configured.
    """
    slack_api_token = env_vars.get_slack_api_token()
    slack_channel = env_vars.get_slack_channel()
    test_identifier = env_vars.get_test_identifier()

    if slack_api_token is None or slack_channel is None:
        yield None
        return

    logger.info(f"[{operation_name}] Resolving slack channel {slack_channel} to ID")
    slack_channel_id = slack_get_channel_id_by_name(slack_api_token, slack_channel)
    logger.info(
        f"[{operation_name}] Resolved slack channel {slack_channel} to ID {slack_channel_id}"
    )

    if slack_channel_id is None:
        logger.error(f"[{operation_name}] Could not find channel {slack_channel}")
        return

    config = SlackConfig(
        token=slack_api_token,
        channel=slack_channel_id,
        test_identifier=test_identifier,
    )

    try:
        yield config
    except Exception as e:
        logger.error(
            f"Failed to {operation_name} to slack channel {slack_channel}: {e}"
        )


class TestProgressTracker:
    """Track test progress and send updates to Slack."""

    def __init__(self, auth_session):
        self.total_tests = 0
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.last_update_time = 0.0
        # tuples of test nodeid and User visible description of the test that failed
        self.failed_tests = set()
        self.min_update_interval = 10  # Minimum seconds between updates
        self.update_frequency = 5  # Send update every N tests
        self._counted_tests: dict[str, str] = {}  # nodeid -> outcome
        self.datahub_stats: dict[str, Any] = {}
        self._slack_thread_ts: str | None = env_vars.get_slack_thread_ts()
        self._main_msg_ts: str | None = None
        self.default_channel_name: str | None = None
        if env_vars.get_notify_customers():
            self.default_channel_name = get_default_channel_name(auth_session)

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

    def _capture_thread_ts(self, response: SlackResponse) -> None:
        """Capture thread timestamp from Slack response if not already set."""
        if self._slack_thread_ts is None and response.get("ok"):
            self._slack_thread_ts = response.get("ts")
            logger.info(f"Created new Slack thread: {self._slack_thread_ts}")

    def send_update_if_needed(self):
        if self.should_send_update():
            logger.info("Sending Slack update for Cypress test progress")
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
        """Send initial message when test collection completes."""
        with slack_operation("send collection message") as config:
            if config is None:
                return

            message = (
                f"{config.test_identifier} - Collected {self.total_tests} tests. Starting test run..."
                + self._format_multi_timezone_timestamp()
            )

            response = slack_send_message(
                token=config.token,
                channel=config.channel,
                message=message,
                thread_ts=self._slack_thread_ts,
            )

            self._capture_thread_ts(response)
            # Store the main message timestamp so we can update it later
            if response.get("ok"):
                self._main_msg_ts = response.get("ts")
                logger.info(
                    f"Initial message posted - ts: {self._main_msg_ts}, "
                    f"channel from response: {response.get('channel')}, "
                    f"channel used in request: {config.channel}"
                )

    def send_progress_message(self) -> None:
        """Update the main message with progress information."""
        try:
            with slack_operation("send progress update") as config:
                completed = self.passed + self.failed + self.skipped
                progress_pct = (
                    (completed / self.total_tests * 100) if self.total_tests > 0 else 0
                )

                message = (
                    "Smoke test progress update:\n"
                    f"Completed: {completed}/{self.total_tests} ({progress_pct:.1f}%)\n"
                    f"✅ Passed: {self.passed}\n"
                    f"❌ Failed: {self.failed}\n"
                    f"⏭️  Skipped: {self.skipped}"
                    + self._format_multi_timezone_timestamp()
                )
                logger.info(f"{message}")

                if config is None:
                    return

                if self._main_msg_ts is None:
                    logger.warning(
                        "No main message timestamp available for progress update"
                    )
                    return

                logger.debug(
                    f"Attempting to update message - ts: {self._main_msg_ts}, "
                    f"channel: {config.channel}"
                )

                # Update existing main message
                response = slack_update_message(
                    token=config.token,
                    channel=config.channel,
                    ts=self._main_msg_ts,
                    message=message,
                )

                logger.info(
                    f"Update response - ok: {response.get('ok')}, "
                    f"error: {response.get('error')}"
                )
        except Exception:
            # Not stopping the tests on account oa progress message failure.
            logger.exception(
                "Failed to send progress update in internal release channel:"
            )

    def send_final_message(self, exitstatus: int) -> None:
        """Update the main message with final test results."""

        self.send_final_message_via_integrations_service(exitstatus)
        try:
            with slack_operation("send final message") as config:
                message = self._build_test_results_message()
                logger.info(f"Final status message\n{message}\n")
                if config is None:
                    return

                if self._main_msg_ts is None:
                    logger.warning(
                        "No main message timestamp available for final update"
                    )
                    return

                # lets not look at exit status for reporting since that may also capture non-test failures. For ex a misconfigured internal slack
                # we still have detailed logs
                passed_str = "PASSED" if self.failed == 0 else "FAILED"
                full_message = (
                    f"{config.test_identifier} Status - {passed_str}\n{message}"
                    + self._format_multi_timezone_timestamp()
                )

                logger.info(
                    f"Attempting to update final message - ts: {self._main_msg_ts}, "
                    f"channel: {config.channel}"
                )

                # Update existing main message
                response = slack_update_message(
                    token=config.token,
                    channel=config.channel,
                    ts=self._main_msg_ts,
                    message=full_message,
                )

                logger.info(
                    f"Final update response - ok: {response.get('ok')}, "
                    f"error: {response.get('error')}"
                )
        except Exception:
            logger.exception(
                "Failed to send final slack update in internal release channel"
            )

    def send_final_message_via_integrations_service(self, exitstatus: int) -> None:
        """Send notification via integrations service if configured."""
        notify_customers = env_vars.get_notify_customers()
        logger.info(
            f"send_final_message_via_integrations_service called with "
            f"NOTIFY_CUSTOMERS={notify_customers}"
        )

        if notify_customers != "true":
            logger.info(
                "Skipping notification via integrations service because NOTIFY_CUSTOMERS is not true"
            )
            return None

        try:
            default_channel = self.default_channel_name
            if not default_channel:
                logger.info(
                    "Default channel not configured, cannot send slack notification in customer slack channel"
                )
                return None

            logger.info(
                f"Sending notification via integrations service to "
                f"channel: {default_channel}"
            )

            message_body = self._build_test_results_message(add_datahub_stats=False)
            status_str = "PASSED" if exitstatus == 0 else "FAILED"

            send_integrations_notification(default_channel, message_body, status_str)
        except Exception as e:
            logger.warning(f"Failed to send notification via integrations service: {e}")

    def send_update(self):
        """Send progress update to Slack."""
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
                    _module_tracker = TestProgressTracker(auth_session)
    return _module_tracker


def send_integrations_notification(
    channel_name: str, message: str, status_str: str
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


def set_default_channel_name(channel_name: str) -> None:
    """Set the default channel name for the module tracker."""
    tracker = get_module_tracker()
    tracker.default_channel_name = channel_name
