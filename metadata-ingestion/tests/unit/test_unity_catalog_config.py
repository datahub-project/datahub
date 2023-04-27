from datetime import timedelta, datetime, timezone

import pytest
from pydantic.error_wrappers import ValidationError

from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from freezegun import freeze_time

FROZEN_TIME = datetime.fromisoformat("2023-01-01 00:00:00").astimezone(timezone.utc)


@freeze_time(FROZEN_TIME)
def test_within_thirty_days():
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "workspace_url",
            "include_usage_statistics": True,
            "start_time": FROZEN_TIME - timedelta(days=30),
        }
    )
    assert config.start_time == FROZEN_TIME - timedelta(days=30)

    with pytest.raises(ValidationError):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "workspace_url",
                "include_usage_statistics": True,
                "start_time": FROZEN_TIME - timedelta(days=31),
            }
        )
