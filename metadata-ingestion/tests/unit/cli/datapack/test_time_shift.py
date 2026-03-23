"""Tests for the time-shift utility."""

import json
import pathlib

from datahub.cli.datapack.time_shift import _walk_and_shift, time_shift_file

ONE_DAY_MS = 86400 * 1000


class TestWalkAndShift:
    def test_shift_last_observed(self) -> None:
        data = {"systemMetadata": {"lastObserved": 1000, "runId": "test"}}
        result = _walk_and_shift(data, delta_ms=500)
        assert result["systemMetadata"]["lastObserved"] == 1500
        assert result["systemMetadata"]["runId"] == "test"

    def test_shift_timestamp_millis(self) -> None:
        data = {"timestampMillis": 2000, "value": 42}
        result = _walk_and_shift(data, delta_ms=1000)
        assert result["timestampMillis"] == 3000
        assert result["value"] == 42

    def test_shift_last_updated_timestamp(self) -> None:
        data = {"lastUpdatedTimestamp": 5000}
        result = _walk_and_shift(data, delta_ms=-2000)
        assert result["lastUpdatedTimestamp"] == 3000

    def test_shift_audit_stamp(self) -> None:
        data = {"time": 1000, "actor": "urn:li:corpuser:admin"}
        result = _walk_and_shift(data, delta_ms=500)
        assert result["time"] == 1500
        assert result["actor"] == "urn:li:corpuser:admin"

    def test_no_shift_time_without_actor(self) -> None:
        """'time' alone is too generic -- should not be shifted."""
        data = {"time": 1000, "value": "something"}
        result = _walk_and_shift(data, delta_ms=500)
        assert result["time"] == 1000

    def test_nested_shift(self) -> None:
        data = {
            "aspects": [
                {"timestampMillis": 100},
                {
                    "ownership": {
                        "lastModified": {"time": 200, "actor": "urn:li:corpuser:admin"}
                    }
                },
            ]
        }
        result = _walk_and_shift(data, delta_ms=50)
        assert result["aspects"][0]["timestampMillis"] == 150
        assert result["aspects"][1]["ownership"]["lastModified"]["time"] == 250

    def test_clamp_to_zero(self) -> None:
        data = {"lastObserved": 100}
        result = _walk_and_shift(data, delta_ms=-200)
        assert result["lastObserved"] == 0

    def test_extra_fields(self) -> None:
        data = {"customTime": 1000, "other": "value"}
        result = _walk_and_shift(data, delta_ms=100, extra_fields={"customTime"})
        assert result["customTime"] == 1100

    def test_non_numeric_timestamp_field_ignored(self) -> None:
        data = {"lastObserved": "not-a-number"}
        result = _walk_and_shift(data, delta_ms=100)
        assert result["lastObserved"] == "not-a-number"


class TestTimeShiftFile:
    def test_shift_file(self, tmp_path: pathlib.Path) -> None:
        input_data = [
            {
                "entityUrn": "urn:li:dataset:test",
                "systemMetadata": {"lastObserved": 1000000},
                "aspect": {"timestampMillis": 900000},
            }
        ]
        input_file = tmp_path / "input.json"
        input_file.write_text(json.dumps(input_data))

        result_path = time_shift_file(
            input_path=input_file,
            reference_timestamp=1000000,
            target_timestamp=2000000,
        )

        with open(result_path) as f:
            result = json.load(f)

        assert result[0]["systemMetadata"]["lastObserved"] == 2000000
        assert result[0]["aspect"]["timestampMillis"] == 1900000

    def test_zero_delta_returns_original(self, tmp_path: pathlib.Path) -> None:
        input_file = tmp_path / "input.json"
        input_file.write_text("[]")

        result_path = time_shift_file(
            input_path=input_file,
            reference_timestamp=1000,
            target_timestamp=1000,
        )

        assert result_path == input_file
