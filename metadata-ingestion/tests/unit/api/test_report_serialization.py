"""Tests for Report serialization with various key types."""

import json
import time
from enum import Enum

from datahub.ingestion.api.report import Report
from datahub.ingestion.source_report.ingestion_stage import (
    IngestionHighStage,
    IngestionStageReport,
)
from datahub.utilities.stats_collections import TopKDict


class TestReportTupleKeys:
    """Test Report.to_pure_python_obj() with tuple keys."""

    def test_simple_tuple_keys(self):
        """Test that simple tuple keys are converted to strings."""
        input_dict = {("a", "b"): 1, ("c", "d"): 2}
        result = Report.to_pure_python_obj(input_dict)

        # Tuple keys should be converted to string representation
        assert "('a', 'b')" in result
        assert result["('a', 'b')"] == 1
        assert "('c', 'd')" in result
        assert result["('c', 'd')"] == 2

        assert json.dumps(result)

    def test_nested_tuple_keys(self):
        """Test that nested dicts with tuple keys are handled."""
        input_dict = {
            "outer": {("inner", "tuple"): "value"},
            ("outer", "tuple"): {"inner": "value2"},
        }
        result = Report.to_pure_python_obj(input_dict)

        # Both levels should have tuple keys converted
        assert "outer" in result
        assert "('inner', 'tuple')" in result["outer"]
        assert "('outer', 'tuple')" in result

        assert json.dumps(result)

    def test_topkdict_with_tuple_keys(self):
        """Test TopKDict with tuple keys."""
        tk: TopKDict = TopKDict()
        tk[("stage1", "substage1")] = 1.5
        tk[("stage2", "substage2")] = 2.5

        result = Report.to_pure_python_obj(tk)

        # TopKDict.as_obj() returns a regular dict, then our code converts tuple keys
        assert isinstance(result, dict)
        assert "('stage1', 'substage1')" in result
        assert result["('stage1', 'substage1')"] == 1.5

        assert json.dumps(result)


class TestReportEnumKeys:
    """Test Report.to_pure_python_obj() with enum keys."""

    def test_enum_keys_converted_to_names(self):
        """Test that enum keys are converted to their names."""

        class TestEnum(Enum):
            VALUE1 = "value1"
            VALUE2 = "value2"

        input_dict = {TestEnum.VALUE1: 1, TestEnum.VALUE2: 2}
        result = Report.to_pure_python_obj(input_dict)

        # Enum keys should be converted to their names (for backward compatibility)
        assert "VALUE1" in result
        assert result["VALUE1"] == 1
        assert "VALUE2" in result
        assert result["VALUE2"] == 2

        assert json.dumps(result)

    def test_ingestion_high_stage_enum_keys(self):
        """Test IngestionHighStage enum keys (actual use case)."""
        input_dict = {
            IngestionHighStage._UNDEFINED: 10.5,
            IngestionHighStage.PROFILING: 5.2,
        }
        result = Report.to_pure_python_obj(input_dict)

        # Should have enum names as keys (for backward compatibility)
        assert "_UNDEFINED" in result  # IngestionHighStage._UNDEFINED.name
        assert "PROFILING" in result  # IngestionHighStage.PROFILING.name

        assert json.dumps(result)


class TestIngestionStageReportSerialization:
    """Integration tests with actual IngestionStageReport."""

    def test_ingestion_stage_report_json_serializable(self):
        """Test that IngestionStageReport produces JSON-serializable output via Report.to_pure_python_obj()."""
        report = IngestionStageReport()

        # Create some stage data (simulates real usage)
        with report.new_stage("Test Stage 1"):
            time.sleep(0.01)
        with report.new_stage("Test Stage 2"):
            time.sleep(0.01)

        # Convert using Report.to_pure_python_obj (IngestionStageReport is a dataclass, not a Report)
        report_dict = Report.to_pure_python_obj(report)

        # Should be JSON serializable (this was failing before the fix)
        json_str = json.dumps(report_dict)
        assert json_str

        # Verify structure
        parsed = json.loads(json_str)
        assert "ingestion_stage_durations" in parsed
        assert "ingestion_high_stage_seconds" in parsed

    def test_ingestion_stage_durations_have_string_keys(self):
        """Test that ingestion_stage_durations keys are strings after conversion."""
        report = IngestionStageReport()

        with report.new_stage("Test Stage"):
            time.sleep(0.01)

        report_dict = Report.to_pure_python_obj(report)

        # All keys should be strings
        for key in report_dict.get("ingestion_stage_durations", {}):
            assert isinstance(key, str), f"Key should be string, got {type(key)}"

    def test_high_stage_seconds_have_string_keys(self):
        """Test that ingestion_high_stage_seconds keys are strings after conversion."""
        report = IngestionStageReport()
        report.ingestion_high_stage_seconds[IngestionHighStage.PROFILING] = 10.0

        report_dict = Report.to_pure_python_obj(report)

        # All keys should be strings (enum names)
        for key in report_dict.get("ingestion_high_stage_seconds", {}):
            assert isinstance(key, str), f"Key should be string, got {type(key)}"


class TestBackwardCompatibility:
    """Ensure fix doesn't break existing functionality."""

    def test_existing_string_keys_unchanged(self):
        """Test that existing behavior with string keys is preserved."""
        input_dict = {"key1": "value1", "key2": 2, "key3": {"nested": "value"}}
        result = Report.to_pure_python_obj(input_dict)

        assert result == input_dict
        assert json.dumps(result)

    def test_existing_int_keys_converted_to_strings(self):
        """Test that int keys are converted to strings (for JSON compatibility)."""
        input_dict = {1: "one", 2: "two", 3: "three"}
        result = Report.to_pure_python_obj(input_dict)

        # Integer keys are converted to strings for JSON compatibility
        assert "1" in result
        assert "2" in result
        assert "3" in result
        assert json.dumps(result)

    def test_mixed_key_types(self):
        """Test dict with mixed key types."""
        input_dict = {
            "string": 1,
            123: 2,
            ("tuple", "key"): 3,
        }
        result = Report.to_pure_python_obj(input_dict)

        assert "string" in result
        assert "123" in result  # Integer keys are converted to strings
        assert "('tuple', 'key')" in result
        assert json.dumps(result)


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_dict(self):
        """Test converting empty dictionary."""
        assert Report.to_pure_python_obj({}) == {}

    def test_none_values_filtered(self):
        """Test that None values are filtered out."""
        input_dict = {"key1": "value1", "key2": None, ("tuple",): "value2"}
        result = Report.to_pure_python_obj(input_dict)

        assert "key1" in result
        assert "key2" not in result
        assert "('tuple',)" in result

    def test_nested_tuples_in_keys(self):
        """Test tuples containing tuples as keys."""
        input_dict = {(("nested",), "tuple"): "value"}
        result = Report.to_pure_python_obj(input_dict)

        # Should convert to string representation
        assert "(('nested',), 'tuple')" in result
        assert json.dumps(result)

    def test_enum_with_int_values(self):
        """Test enum keys with integer values."""

        class IntEnum(Enum):
            FIRST = 1
            SECOND = 2

        input_dict = {IntEnum.FIRST: "first", IntEnum.SECOND: "second"}
        result = Report.to_pure_python_obj(input_dict)

        # Should use the enum name (for backward compatibility)
        assert "FIRST" in result
        assert "SECOND" in result
        assert json.dumps(result)
