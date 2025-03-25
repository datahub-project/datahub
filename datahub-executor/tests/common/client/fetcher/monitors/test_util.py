import hashlib

from datahub_executor.common.client.fetcher.monitors.util import (
    get_monitor_training_schedule_with_jitter,
)


def test_get_monitor_training_schedule_with_jitter() -> None:
    """Test that jitter function generates correct cron schedules."""
    # Given
    monitor_urn = "urn:li:monitor:test-monitor"

    # When
    cron_schedule = get_monitor_training_schedule_with_jitter(monitor_urn)

    # Then
    # Calculate expected minute based on hash
    expected_hash = int(hashlib.md5(monitor_urn.encode()).hexdigest(), 16)
    expected_minute = expected_hash % 60
    expected_cron = f"{expected_minute} * * * *"

    assert cron_schedule == expected_cron

    # Verify format matches cron pattern (minute hour day month weekday)
    parts = cron_schedule.split()
    assert len(parts) == 5
    assert 0 <= int(parts[0]) <= 59  # Minute between 0-59
    assert parts[1] == "*"  # Hour is *
    assert parts[2] == "*"  # Day is *
    assert parts[3] == "*"  # Month is *
    assert parts[4] == "*"  # Weekday is *
