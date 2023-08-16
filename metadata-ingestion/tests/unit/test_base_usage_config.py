from datetime import datetime, timezone

from freezegun import freeze_time

from datahub.ingestion.source.usage.usage_common import BaseUsageConfig

FROZEN_TIME = "2023-08-03 09:00:00"
FROZEN_TIME2 = "2023-08-03 09:10:00"


@freeze_time(FROZEN_TIME)
def test_relative_start_time_aligns_with_bucket_start_time():
    config = BaseUsageConfig.parse_obj(
        {"start_time": "-2 days", "end_time": "2023-07-07T09:00:00Z"}
    )
    assert config.start_time == datetime(2023, 7, 5, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 7, 7, 9, tzinfo=timezone.utc)

    config = BaseUsageConfig.parse_obj(
        {"start_time": "-2 days", "end_time": "2023-07-07T09:00:00Z"}
    )
    assert config.start_time == datetime(2023, 7, 5, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 7, 7, 9, tzinfo=timezone.utc)


@freeze_time(FROZEN_TIME)
def test_absolute_start_time_aligns_with_bucket_start_time():
    config = BaseUsageConfig.parse_obj({"start_time": "2023-07-01T00:00:00Z"})
    assert config.start_time == datetime(2023, 7, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)

    config = BaseUsageConfig.parse_obj({"start_time": "2023-07-01T09:00:00Z"})
    assert config.start_time == datetime(2023, 7, 1, 0, tzinfo=timezone.utc)
    assert config.end_time == datetime(2023, 8, 3, 9, tzinfo=timezone.utc)
