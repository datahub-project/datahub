from datetime import datetime, timedelta

import pytest
from freezegun import freeze_time

from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

FROZEN_TIME = datetime.fromisoformat("2023-01-01 00:00:00+00:00")


@freeze_time(FROZEN_TIME)
def test_within_thirty_days():
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "include_usage_statistics": True,
            "start_time": FROZEN_TIME - timedelta(days=30),
        }
    )
    assert config.start_time == FROZEN_TIME - timedelta(days=30)

    with pytest.raises(
        ValueError, match="Query history is only maintained for 30 days."
    ):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "https://workspace_url",
                "include_usage_statistics": True,
                "start_time": FROZEN_TIME - timedelta(days=31),
            }
        )


def test_profiling_requires_warehouses_id():
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "profiling": {
                "enabled": True,
                "method": "ge",
                "warehouse_id": "my_warehouse_id",
            },
        }
    )
    assert config.profiling.enabled is True

    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "profiling": {"enabled": False, "method": "ge"},
        }
    )
    assert config.profiling.enabled is False

    with pytest.raises(ValueError):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "workspace_url",
            }
        )


@freeze_time(FROZEN_TIME)
def test_workspace_url_should_start_with_https():

    with pytest.raises(ValueError, match="Workspace URL must start with http scheme"):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "workspace_url",
                "profiling": {"enabled": True},
            }
        )
