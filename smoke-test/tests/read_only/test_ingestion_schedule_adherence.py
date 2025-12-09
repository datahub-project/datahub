"""Test ingestion schedule adherence - verifies ingestions are running on time.

This read-only test checks that scheduled ingestion sources are executing
according to their configured cron schedules. It verifies:
1. Ingestions with schedules have executed at least once
2. The last execution time is within ±15 minutes of the expected schedule
3. Reports all ingestions that are late or have never run
4. Skips checks for ingestions modified recently (grace period = one schedule interval)
5. Logs warnings (but does not fail) for unparseable cron schedules
6. Handles in-progress executions by checking the most recent completed execution
"""

import logging
from datetime import datetime, timezone as dt_timezone
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pytest
from croniter import croniter

from tests.test_result_msg import add_datahub_stats
from tests.utilities.metadata_operations import (
    get_entity,
    list_scheduled_ingestion_sources,
)

logger = logging.getLogger(__name__)

# Tolerance window: ±15 minutes (in milliseconds)
SCHEDULE_TOLERANCE_MS = 15 * 60 * 1000


def calculate_expected_last_run(
    cron_schedule: str, timezone_str: str, current_time_ms: int
) -> int:
    """Calculate when the ingestion should have last run based on cron schedule.

    Args:
        cron_schedule: Cron expression (e.g., "0 */6 * * *")
        timezone_str: Timezone string (e.g., "UTC", "America/Los_Angeles")
        current_time_ms: Current time in milliseconds since epoch

    Returns:
        Expected last run time in milliseconds since epoch
    """
    try:
        # Convert current time from ms to seconds and apply the ingestion's timezone
        current_time_sec = current_time_ms // 1000
        tz = ZoneInfo(timezone_str)
        current_dt = datetime.fromtimestamp(current_time_sec, tz=tz)

        # Create croniter instance with the timezone-aware datetime
        cron = croniter(cron_schedule, current_dt)

        # Get the previous scheduled run time
        prev_run_dt = cron.get_prev(datetime)

        # Convert back to milliseconds
        return int(prev_run_dt.timestamp() * 1000)
    except Exception as e:
        logger.error(
            f"Failed to calculate expected run time for cron '{cron_schedule}' "
            f"in timezone '{timezone_str}': {e}"
        )
        return -1


def get_last_modified_time(
    ingestion_source_info: Dict,
) -> int | None:
    """Extract last modified time from ingestion source info.

    Uses systemMetadata.aspectModified.time if available, otherwise falls back
    to the created.time field.

    Args:
        ingestion_source_info: The dataHubIngestionSourceInfo aspect

    Returns:
        Last modified time in milliseconds since epoch, or None if not available
    """
    system_metadata = ingestion_source_info.get("systemMetadata", {})
    aspect_modified = system_metadata.get("aspectModified", {})
    modified_time = aspect_modified.get("time")

    if modified_time:
        return modified_time

    # Fall back to created time if modified time not available
    created = ingestion_source_info.get("created", {})
    return created.get("time")


def validate_execution_result(
    latest_execution: Dict, name: str, urn: str
) -> Tuple[bool, str | None, int | None]:
    """Validate that an execution has a result and start time.

    Args:
        latest_execution: The most recent execution request
        name: Ingestion source name
        urn: Ingestion source URN

    Returns:
        Tuple of (is_valid, error_message, start_time_ms)
        - is_valid: True if execution has valid result with start time
        - error_message: Error description if invalid, None otherwise
        - start_time_ms: Start time in ms if valid, None otherwise
    """
    execution_result = latest_execution.get("result")

    if not execution_result:
        return (
            False,
            f"NO RESULT: {name} ({urn}) - Latest execution has no result",
            None,
        )

    last_run_time_ms = execution_result.get("startTimeMs")

    if last_run_time_ms is None:
        return (
            False,
            f"NO START TIME: {name} ({urn}) - Latest execution has no start time",
            None,
        )

    return True, None, last_run_time_ms


def format_schedule_violation_message(
    name: str,
    urn: str,
    last_run_time_ms: int,
    expected_last_run_ms: int,
    cron_interval: str,
    timezone_str: str,
) -> str:
    """Format a detailed message for schedule violations (late or early).

    Args:
        name: Ingestion source name
        urn: Ingestion source URN
        last_run_time_ms: Actual run time in milliseconds
        expected_last_run_ms: Expected run time in milliseconds
        cron_interval: Cron schedule string
        timezone_str: Timezone string

    Returns:
        Formatted error message
    """
    # Format timestamps for readability
    expected_time = datetime.fromtimestamp(
        expected_last_run_ms / 1000, tz=dt_timezone.utc
    ).isoformat()
    actual_time = datetime.fromtimestamp(
        last_run_time_ms / 1000, tz=dt_timezone.utc
    ).isoformat()

    # Check if the ingestion is late (last run was before expected time)
    if last_run_time_ms < expected_last_run_ms:
        minutes_late = (expected_last_run_ms - last_run_time_ms) // 1000 // 60
        return (
            f"LATE: {name} ({urn}) is {minutes_late} minutes late. "
            f"Expected to run at {expected_time}, but last ran at "
            f"{actual_time} (schedule: '{cron_interval}', "
            f"timezone: {timezone_str})"
        )
    else:
        # Last run was after expected time (but too far)
        minutes_early = (last_run_time_ms - expected_last_run_ms) // 1000 // 60
        return (
            f"EARLY: {name} ({urn}) ran {minutes_early} minutes early. "
            f"Expected to run at {expected_time}, but last ran at "
            f"{actual_time} (schedule: '{cron_interval}', "
            f"timezone: {timezone_str})"
        )


def check_never_run_ingestion(
    name: str,
    urn: str,
    expected_last_run_ms: int,
    current_time_ms: int,
    cron_interval: str,
    timezone_str: str,
    last_modified_time_ms: int | None = None,
) -> Tuple[bool, str]:
    """Check if an ingestion that has never run should have run by now.

    Args:
        name: Ingestion source name
        urn: Ingestion source URN
        expected_last_run_ms: When it should have last run
        current_time_ms: Current time in milliseconds
        cron_interval: Cron schedule string
        timezone_str: Timezone string
        last_modified_time_ms: When the ingestion was last modified

    Returns:
        Tuple of (is_on_schedule, reason)
    """
    # Skip check if expected run is before ingestion was last modified
    if last_modified_time_ms and expected_last_run_ms < last_modified_time_ms:
        return (
            True,
            f"Ingestion modified after expected run time: {name} ({urn})",
        )

    time_since_expected_run_ms = current_time_ms - expected_last_run_ms

    if time_since_expected_run_ms > 0:
        minutes_ago = time_since_expected_run_ms // 1000 // 60
        return (
            False,
            f"NEVER RUN: {name} ({urn}) should have run {minutes_ago} "
            f"minutes ago based on schedule '{cron_interval}' "
            f"(timezone: {timezone_str})",
        )
    else:
        # Expected run is in the future, so it's OK that it hasn't run yet
        return True, f"Not yet time for first run: {name} ({urn})"


def check_schedule_adherence(
    auth_session,
    ingestion_source: Dict,
) -> Tuple[bool, str]:
    """Check if an ingestion source is adhering to its schedule.

    Args:
        ingestion_source: Ingestion source data with schedule and executions

    Returns:
        Tuple of (is_on_schedule: bool, reason: str)
    """
    urn = ingestion_source.get("urn", "UNKNOWN")
    name = ingestion_source.get("name", "UNKNOWN")

    # Check if schedule exists
    schedule = ingestion_source.get("schedule")
    if not schedule:
        return True, f"No schedule configured for {name} ({urn})"

    cron_interval = schedule.get("interval")
    timezone_str = schedule.get("timezone", "UTC")

    if not cron_interval:
        return True, f"No cron interval configured for {name} ({urn})"

    # Get executions and filter for scheduled ones only
    executions = ingestion_source.get("executions", {})
    all_execution_requests = executions.get("executionRequests", [])

    # Filter for only scheduled executions (ignore manual/CLI triggers)
    execution_requests = [
        req
        for req in all_execution_requests
        if req.get("input", {}).get("source", {}).get("type")
        == "SCHEDULED_INGESTION_SOURCE"
    ]

    current_time_ms = int(datetime.now(dt_timezone.utc).timestamp() * 1000)

    # Calculate when the ingestion should have last run
    expected_last_run_ms = calculate_expected_last_run(
        cron_interval, timezone_str, current_time_ms
    )

    if expected_last_run_ms < 0:
        return (
            True,
            f"PARSE WARNING: Failed to parse cron schedule for {name} ({urn}): "
            f"schedule='{cron_interval}', timezone='{timezone_str}'",
        )

    # Get last modified time from ingestion source info (needed for both paths)
    entity_obj = get_entity(
        auth_session=auth_session,
        urn=urn,
    )
    ingestion_source_info = entity_obj.get("aspects", {}).get(
        "dataHubIngestionSourceInfo", {}
    )
    last_modified_time_ms = get_last_modified_time(ingestion_source_info)

    # Handle case with no executions
    if not execution_requests:
        return check_never_run_ingestion(
            name,
            urn,
            expected_last_run_ms,
            current_time_ms,
            cron_interval,
            timezone_str,
            last_modified_time_ms,
        )

    # Avoid false failures: long-running ingestions may still be executing
    # without results yet. Use the most recent completed execution instead
    # of assuming the latest has finished.
    completed_execution = None
    for execution in execution_requests:
        execution_result = execution.get("result")
        if execution_result and execution_result.get("startTimeMs") is not None:
            completed_execution = execution
            break

    if not completed_execution:
        return (
            False,
            f"NO COMPLETED EXECUTIONS: {name} ({urn}) - "
            f"All {len(execution_requests)} execution(s) are "
            f"in-progress or have no result",
        )

    # Extract the actual last run time from the completed execution
    last_run_time_ms = completed_execution.get("result", {}).get("startTimeMs")
    assert last_run_time_ms is not None  # Already validated in the loop above

    # Avoid false failures when schedule intervals increase (e.g., 6h→12h).
    # The old execution pattern may appear "late" under the new schedule timing.
    # Grant one full cycle before expecting adherence to the modified schedule.
    if last_modified_time_ms and last_run_time_ms < last_modified_time_ms:
        # Calculate time since modification
        time_since_modification_ms = current_time_ms - last_modified_time_ms

        # Calculate the schedule interval (time between runs)
        try:
            tz = ZoneInfo(timezone_str)
            current_dt = datetime.fromtimestamp(current_time_ms // 1000, tz=tz)
            cron = croniter(cron_interval, current_dt)
            prev_run_dt = cron.get_prev(datetime)
            prev_prev_run_dt = croniter(cron_interval, prev_run_dt).get_prev(datetime)
            schedule_interval_ms = int(
                (prev_run_dt.timestamp() - prev_prev_run_dt.timestamp()) * 1000
            )

            # If modification is recent (less than one full interval), skip the check
            if time_since_modification_ms < schedule_interval_ms:
                minutes_since_mod = time_since_modification_ms // 1000 // 60
                minutes_interval = schedule_interval_ms // 1000 // 60
                return (
                    True,
                    f"MODIFIED RECENTLY: {name} ({urn}) - Schedule modified "
                    f"{minutes_since_mod} minutes ago. Grace period: "
                    f"{minutes_interval} minutes (one full interval). "
                    f"Waiting for first run under new schedule.",
                )
        except Exception as e:
            logger.warning(
                f"Failed to calculate schedule interval for {name}: {e}. "
                f"Using simple modification check."
            )
            # Fallback: if expected run is before modification, skip check
            if expected_last_run_ms < last_modified_time_ms:
                return (
                    True,
                    f"Ingestion modified after expected run time: {name} ({urn})",
                )

    # Check if last run is within tolerance of the expected run time
    # Tolerance window: [expected - 15min, expected + 15min]
    time_diff_ms = last_run_time_ms - expected_last_run_ms

    if abs(time_diff_ms) <= SCHEDULE_TOLERANCE_MS:
        # Within tolerance - on schedule
        return True, f"ON SCHEDULE: {name} ({urn})"

    # Outside tolerance - check if it might be the next cycle
    # Calculate the next expected run time
    try:
        tz = ZoneInfo(timezone_str)
        expected_dt = datetime.fromtimestamp(expected_last_run_ms // 1000, tz=tz)
        cron = croniter(cron_interval, expected_dt)
        next_run_dt = cron.get_next(datetime)
        next_expected_ms = int(next_run_dt.timestamp() * 1000)

        # Check if within tolerance of next cycle
        next_time_diff_ms = last_run_time_ms - next_expected_ms
        if abs(next_time_diff_ms) <= SCHEDULE_TOLERANCE_MS:
            # Actually on time for the next cycle
            return True, f"ON SCHEDULE: {name} ({urn})"
    except Exception as e:
        logger.warning(f"Failed to check next cycle for {name}: {e}")

    # Definitely off schedule
    return (
        False,
        format_schedule_violation_message(
            name,
            urn,
            last_run_time_ms,
            expected_last_run_ms,
            cron_interval,
            timezone_str,
        ),
    )


@pytest.mark.read_only
def test_ingestion_schedule_adherence(auth_session):
    """Test that scheduled ingestions are running on time.

    This test:
    1. Lists all ingestion sources
    2. Filters for those with cron schedules
    3. Checks if they should have run by now based on last modified time + schedule
    4. Verifies the last execution was within ±15 minutes of expected time
    5. Fails if ANY ingestion is late or has never run when it should have
    6. Logs warnings (but does not fail) for unparseable cron schedules

    The test is strict: it will fail if any ingestion is out of schedule,
    but it collects all failures before failing to provide a comprehensive
    report. Ingestions with unparseable cron schedules are reported as
    warnings and tracked separately in statistics.
    """
    logger.info("Starting ingestion schedule adherence test")

    # Fetch all ingestion sources
    res_data = list_scheduled_ingestion_sources(auth_session, start=0, count=1000)
    assert res_data, "Failed to fetch ingestion sources"

    total_sources = res_data.get("total", 0)
    ingestion_sources = res_data.get("ingestionSources", [])

    logger.info(f"Found {total_sources} total ingestion sources")
    add_datahub_stats("total-ingestion-sources", total_sources)

    # Filter for sources that have schedules
    scheduled_sources = [
        source for source in ingestion_sources if source.get("schedule")
    ]

    scheduled_count = len(scheduled_sources)
    logger.info(f"Found {scheduled_count} ingestion sources with schedules configured")
    add_datahub_stats("scheduled-ingestion-sources", len(scheduled_sources))

    if len(scheduled_sources) == 0:
        logger.info(
            "No scheduled ingestion sources found - test passes (nothing to check)"
        )
        return

    # Check schedule adherence for each source
    failures: List[str] = []
    warnings: List[str] = []
    on_schedule_count = 0

    for source in scheduled_sources:
        is_on_schedule, reason = check_schedule_adherence(auth_session, source)

        if is_on_schedule:
            # Check if it's a parse warning
            if reason.startswith("PARSE WARNING:"):
                logger.warning(f"⚠ {reason}")
                warnings.append(reason)
            else:
                logger.info(f"✓ {reason}")
                on_schedule_count += 1
        else:
            logger.error(f"✗ {reason}")
            failures.append(reason)

    # Report statistics
    add_datahub_stats("ingestions-on-schedule", on_schedule_count)
    add_datahub_stats("ingestions-off-schedule", len(failures))
    add_datahub_stats("ingestions-parse-warnings", len(warnings))

    # Fail the test if there are any failures
    if failures:
        failure_report = "\n\n".join([f"  - {failure}" for failure in failures])
        pytest.fail(
            f"\n\n{'=' * 80}\n"
            f"SCHEDULE ADHERENCE TEST FAILED\n"
            f"{'=' * 80}\n\n"
            f"Found {len(failures)} ingestion(s) that are NOT running on schedule:\n\n"
            f"{failure_report}\n\n"
            f"Summary:\n"
            f"  - Total scheduled ingestions: {len(scheduled_sources)}\n"
            f"  - On schedule: {on_schedule_count}\n"
            f"  - Off schedule: {len(failures)}\n"
            f"  - Tolerance window: ±{SCHEDULE_TOLERANCE_MS // 1000 // 60} minutes\n"
            f"\n{'=' * 80}\n"
        )

    if warnings:
        warnings_report = "\n\n".join([f"  - {warning}" for warning in warnings])
        logger.info(
            f"✓ All {on_schedule_count} scheduled ingestion sources are "
            f"running on time!\n\n"
            f"Note: {len(warnings)} ingestion(s) have unparseable cron schedules:\n\n"
            f"{warnings_report}\n"
        )
    else:
        logger.info(
            f"✓ All {len(scheduled_sources)} scheduled ingestion sources are "
            f"running on time!"
        )
