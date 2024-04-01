from datetime import datetime, timezone

import freezegun
import pytest

from datahub.configuration.datetimes import parse_user_datetime


# FIXME: Ideally we'd use tz_offset here to test this code in a non-UTC timezone.
# However, freezegun has a long-standing bug that prevents this from working:
# https://github.com/spulec/freezegun/issues/348.
@freezegun.freeze_time("2021-09-01 10:02:03")
def test_user_time_parser():
    # Absolute times.
    assert parse_user_datetime("2022-01-01 01:02:03 UTC") == datetime(
        2022, 1, 1, 1, 2, 3, tzinfo=timezone.utc
    )
    assert parse_user_datetime("2022-01-01 01:02:03 -02:00") == datetime(
        2022, 1, 1, 3, 2, 3, tzinfo=timezone.utc
    )

    assert parse_user_datetime("2024-03-01 00:46:33.000 -0800") == datetime(
        2024, 3, 1, 8, 46, 33, tzinfo=timezone.utc
    )

    # Times with no timestamp are assumed to be in UTC.
    assert parse_user_datetime("2022-01-01 01:02:03") == datetime(
        2022, 1, 1, 1, 2, 3, tzinfo=timezone.utc
    )
    assert parse_user_datetime("2022-02-03") == datetime(
        2022, 2, 3, tzinfo=timezone.utc
    )

    # Timestamps.
    assert parse_user_datetime("1630440123") == datetime(
        2021, 8, 31, 20, 2, 3, tzinfo=timezone.utc
    )
    assert parse_user_datetime("1630440123837.018") == datetime(
        2021, 8, 31, 20, 2, 3, 837018, tzinfo=timezone.utc
    )

    # Relative times.
    assert parse_user_datetime("10m") == datetime(
        2021, 9, 1, 10, 12, 3, tzinfo=timezone.utc
    )
    assert parse_user_datetime("+ 1 day") == datetime(
        2021, 9, 2, 10, 2, 3, tzinfo=timezone.utc
    )
    assert parse_user_datetime("-2 days") == datetime(
        2021, 8, 30, 10, 2, 3, tzinfo=timezone.utc
    )

    # Invalid inputs.
    with pytest.raises(ValueError):
        parse_user_datetime("invalid")
