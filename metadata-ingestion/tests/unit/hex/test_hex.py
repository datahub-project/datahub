import unittest
from datetime import datetime, timedelta, timezone

from datahub.ingestion.source.hex.hex import HexSourceConfig


def datetime_approx_equal(
    dt1: datetime, dt2: datetime, tolerance_seconds: int = 5
) -> bool:
    if dt1.tzinfo is None:
        dt1 = dt1.replace(tzinfo=timezone.utc)
    if dt2.tzinfo is None:
        dt2 = dt2.replace(tzinfo=timezone.utc)

    diff = abs((dt1 - dt2).total_seconds())
    return diff <= tolerance_seconds


class TestHexSourceConfig(unittest.TestCase):
    def setUp(self):
        self.minimum_input_config = {
            "workspace_name": "test-workspace",
            "token": "test-token",
        }

    def test_required_fields(self):
        with self.assertRaises(ValueError):
            input_config = {**self.minimum_input_config}
            del input_config["workspace_name"]
            HexSourceConfig.parse_obj(input_config)

        with self.assertRaises(ValueError):
            input_config = {**self.minimum_input_config}
            del input_config["token"]
            HexSourceConfig.parse_obj(input_config)

    def test_minimum_config(self):
        config = HexSourceConfig.parse_obj(self.minimum_input_config)

        assert config
        assert config.workspace_name == "test-workspace"
        assert config.token.get_secret_value() == "test-token"

    def test_lineage_config(self):
        config = HexSourceConfig.parse_obj(self.minimum_input_config)
        assert config and config.include_lineage
        config = HexSourceConfig.parse_obj(
            self.minimum_input_config | {"include_lineage": False}
        )
        assert config and not config.include_lineage

        # default values for lineage_start_time and lineage_end_time
        config = HexSourceConfig.parse_obj(self.minimum_input_config)
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime.now(tz=timezone.utc) - timedelta(days=1),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time, datetime.now(tz=timezone.utc)
            )
        )
        # set values for lineage_start_time and lineage_end_time
        config = HexSourceConfig.parse_obj(
            self.minimum_input_config
            | {
                "lineage_start_time": "2025-03-24 12:00:00",
                "lineage_end_time": "2025-03-25 12:00:00",
            }
        )
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime(2025, 3, 24, 12, 0, 0, tzinfo=timezone.utc),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time,
                datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc),
            )
        )
        # set lineage_end_time only
        config = HexSourceConfig.parse_obj(
            self.minimum_input_config | {"lineage_end_time": "2025-03-25 12:00:00"}
        )
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
                - timedelta(days=1),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time,
                datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc),
            )
        )
        # set lineage_start_time only
        config = HexSourceConfig.parse_obj(
            self.minimum_input_config | {"lineage_start_time": "2025-03-25 12:00:00"}
        )
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time, datetime.now(tz=timezone.utc)
            )
        )
        # set relative times for lineage_start_time and lineage_end_time
        config = HexSourceConfig.parse_obj(
            self.minimum_input_config
            | {"lineage_start_time": "-3day", "lineage_end_time": "now"}
        )
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime.now(tz=timezone.utc) - timedelta(days=3),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time, datetime.now(tz=timezone.utc)
            )
        )
