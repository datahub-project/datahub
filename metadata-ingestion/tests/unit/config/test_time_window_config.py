from datetime import datetime, timezone

import pytest
from freezegun import freeze_time

from datahub.configuration.time_window_config import BaseTimeWindowConfig

FROZEN_TIME = "2023-08-03 09:00:00"
FROZEN_TIME2 = "2023-08-03 09:10:00"


@freeze_time(FROZEN_TIME)
def test_default_start_end_time():
    config = BaseTimeWindowConfig.parse_obj({})
    assert config.start_time == datetime(2023, 8, 2, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)


@freeze_time(FROZEN_TIME2)
def test_default_start_end_time_hour_bucket_duration():
    config = BaseTimeWindowConfig.parse_obj({"bucket_duration": "HOUR"})
    assert config.start_time == datetime(2023, 8, 3, 8, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, 10, tzinfo=timezone.utc)


@freeze_time(FROZEN_TIME)
def test_relative_start_time():
    config = BaseTimeWindowConfig.parse_obj({"start_time": "-2 days"})
    assert config.start_time == datetime(2023, 8, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseTimeWindowConfig.parse_obj({"start_time": "-2d"})
    assert config.start_time == datetime(2023, 8, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseTimeWindowConfig.parse_obj(
        {"start_time": "-2 days", "end_time": "2023-07-07T09:00:00Z"}
    )
    assert config.start_time == datetime(2023, 7, 5, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 7, 7, 9, tzinfo=timezone.utc)

    config = BaseTimeWindowConfig.parse_obj(
        {"start_time": "-2 days", "end_time": "2023-07-07T09:00:00Z"}
    )
    assert config.start_time == datetime(2023, 7, 5, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 7, 7, 9, tzinfo=timezone.utc)


@freeze_time(FROZEN_TIME)
def test_absolute_start_time():
    config = BaseTimeWindowConfig.parse_obj({"start_time": "2023-07-01T00:00:00Z"})
    assert config.start_time == datetime(2023, 7, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseTimeWindowConfig.parse_obj({"start_time": "2023-07-01T09:00:00Z"})
    assert config.start_time == datetime(2023, 7, 1, 9, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)


@freeze_time(FROZEN_TIME)
def test_invalid_relative_start_time():
    with pytest.raises(ValueError, match="Unknown string format"):
        BaseTimeWindowConfig.parse_obj({"start_time": "-2 das"})

    with pytest.raises(
        ValueError,
        match="Relative start time should be in terms of configured bucket duration",
    ):
        BaseTimeWindowConfig.parse_obj({"start_time": "-2"})

    with pytest.raises(
        ValueError, match="Relative start time should start with minus sign"
    ):
        BaseTimeWindowConfig.parse_obj({"start_time": "2d"})

    with pytest.raises(
        ValueError,
        match="Relative start time should be in terms of configured bucket duration",
    ):
        BaseTimeWindowConfig.parse_obj({"start_time": "-2m"})
