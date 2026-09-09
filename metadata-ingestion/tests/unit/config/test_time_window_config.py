import re
from datetime import datetime, timezone

import pytest
import time_machine

from datahub.configuration.time_window_config import BaseTimeWindowConfig

FROZEN_TIME = "2023-08-03 09:00:00+00:00"
FROZEN_TIME2 = "2023-08-03 09:10:00+00:00"


@time_machine.travel(FROZEN_TIME, tick=False)
def test_default_start_end_time():
    config = BaseTimeWindowConfig.model_validate({})
    assert config.start_time == datetime(2023, 8, 2, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)


@time_machine.travel(FROZEN_TIME2, tick=False)
def test_default_start_end_time_hour_bucket_duration():
    config = BaseTimeWindowConfig.model_validate({"bucket_duration": "HOUR"})
    assert config.start_time == datetime(2023, 8, 3, 8, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, 10, tzinfo=timezone.utc)


@time_machine.travel(FROZEN_TIME, tick=False)
def test_relative_start_time():
    config = BaseTimeWindowConfig.model_validate({"start_time": "-2 days"})
    assert config.start_time == datetime(2023, 8, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseTimeWindowConfig.model_validate({"start_time": "-2d"})
    assert config.start_time == datetime(2023, 8, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseTimeWindowConfig.model_validate(
        {"start_time": "-2 days", "end_time": "2023-07-07T09:00:00Z"}
    )
    assert config.start_time == datetime(2023, 7, 5, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 7, 7, 9, tzinfo=timezone.utc)

    config = BaseTimeWindowConfig.model_validate(
        {"start_time": "-2 days", "end_time": "2023-07-07T09:00:00Z"}
    )
    assert config.start_time == datetime(2023, 7, 5, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 7, 7, 9, tzinfo=timezone.utc)


@time_machine.travel(FROZEN_TIME, tick=False)
def test_absolute_start_time():
    config = BaseTimeWindowConfig.model_validate({"start_time": "2023-07-01T00:00:00Z"})
    assert config.start_time == datetime(2023, 7, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseTimeWindowConfig.model_validate({"start_time": "2023-07-01T09:00:00Z"})
    assert config.start_time == datetime(2023, 7, 1, 9, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)


@time_machine.travel(FROZEN_TIME, tick=False)
def test_invalid_relative_start_time():
    with pytest.raises(ValueError, match="Unknown string format"):
        BaseTimeWindowConfig.model_validate({"start_time": "-2 das"})

    with pytest.raises(
        ValueError,
        match="Relative start time should be in terms of configured bucket duration",
    ):
        BaseTimeWindowConfig.model_validate({"start_time": "-2"})

    with pytest.raises(
        ValueError, match="Relative start time should start with minus sign"
    ):
        BaseTimeWindowConfig.model_validate({"start_time": "2d"})

    with pytest.raises(
        ValueError,
        match="Relative start time should be in terms of configured bucket duration",
    ):
        BaseTimeWindowConfig.model_validate({"start_time": "-2m"})


def test_start_time_json_schema_allows_relative_timespans():
    # The generated JSON schema for start_time must permit relative timespans
    # like "-7d", not only ISO 8601 datetimes (see RelativeOrAbsoluteDatetime).
    props = BaseTimeWindowConfig.model_json_schema()["properties"]
    branches = props["start_time"]["anyOf"]

    # Absolute ISO 8601 datetimes remain described.
    assert any(branch.get("format") == "date-time" for branch in branches)

    # Relative timespans match the dedicated pattern branch.
    patterns = [branch["pattern"] for branch in branches if "pattern" in branch]
    assert patterns, "start_time schema is missing the relative-timespan branch"
    pattern = patterns[0]
    assert re.fullmatch(pattern, "-7d")
    assert re.fullmatch(pattern, "-7 days")
    assert re.fullmatch(pattern, "-2h")
    assert not re.fullmatch(pattern, "7d")  # leading minus sign is required
    assert not re.fullmatch(pattern, "-42 hello")
