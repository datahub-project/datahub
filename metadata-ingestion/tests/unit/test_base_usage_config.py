from datetime import datetime, timezone

from freezegun import freeze_time

from datahub.ingestion.source.usage.usage_common import BaseUsageConfig

FROZEN_TIME = "2023-08-03 09:00:00"


@freeze_time(FROZEN_TIME)
def test_default_start_end_time():
    config = BaseUsageConfig.parse_obj({})
    assert config.start_time == datetime(2023, 8, 2, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)


@freeze_time(FROZEN_TIME)
def test_relative_start_time():
    config = BaseUsageConfig.parse_obj({"start_time": "-2 days"})
    assert config.start_time == datetime(2023, 8, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseUsageConfig.parse_obj({"start_time": "-2d"})
    assert config.start_time == datetime(2023, 8, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)


@freeze_time(FROZEN_TIME)
def test_absolute_start_time():
    config = BaseUsageConfig.parse_obj({"start_time": "2023-07-01T00:00:00Z"})
    assert config.start_time == datetime(2023, 7, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseUsageConfig.parse_obj({"start_time": "2023-07-01T09:00:00Z"})
    assert config.start_time == datetime(2023, 7, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)
