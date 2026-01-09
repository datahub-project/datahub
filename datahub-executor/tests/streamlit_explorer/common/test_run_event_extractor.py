"""Tests for the run_event_extractor module."""

from datetime import datetime, timezone

import pandas as pd
import pytest

from scripts.streamlit_explorer.common.run_event_extractor import (
    ASSERTION_TYPE_VALUE_COLUMNS,
    METRIC_VALUE_COLUMNS,
    TIMESTAMP_COLUMN,
    AssertionMetadata,
    MonitorMetadata,
    extract_entity_from_monitor_urn,
    extract_timeseries,
    extract_timeseries_with_bounds,
    filter_events_by_time,
    get_assertion_metadata,
    get_assertion_types,
    get_result_type_counts,
    get_time_range,
    list_assertions,
    list_monitors,
)


class TestAssertionMetadata:
    """Tests for AssertionMetadata dataclass."""

    def test_dataclass_creation(self):
        metadata = AssertionMetadata(
            assertion_urn="urn:li:assertion:123",
            assertee_urn="urn:li:dataset:456",
            assertion_type="FIELD",
            metric_name="null_count",
            field_path="column1",
            point_count=100,
            first_event=datetime(2024, 1, 1, tzinfo=timezone.utc),
            last_event=datetime(2024, 1, 31, tzinfo=timezone.utc),
            value_min=0.0,
            value_max=10.0,
            value_mean=5.0,
        )

        assert metadata.assertion_urn == "urn:li:assertion:123"
        assert metadata.assertion_type == "FIELD"
        assert metadata.point_count == 100

    def test_dataclass_with_none_values(self):
        metadata = AssertionMetadata(
            assertion_urn="urn:li:assertion:123",
            assertee_urn=None,
            assertion_type=None,
            metric_name=None,
            field_path=None,
            point_count=0,
            first_event=None,
            last_event=None,
            value_min=None,
            value_max=None,
            value_mean=None,
        )

        assert metadata.assertion_urn == "urn:li:assertion:123"
        assert metadata.assertee_urn is None
        assert metadata.point_count == 0


class TestListAssertions:
    """Tests for list_assertions function."""

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = list_assertions(df)
        assert result == []

    def test_none_dataframe(self):
        result = list_assertions(None)  # type: ignore[arg-type]
        assert result == []

    def test_missing_assertion_urn_column(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000],
                "status": ["COMPLETE", "COMPLETE"],
            }
        )
        result = list_assertions(df)
        assert result == []

    def test_single_assertion(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": [
                    "urn:li:assertion:1",
                    "urn:li:assertion:1",
                    "urn:li:assertion:1",
                ],
                "asserteeUrn": [
                    "urn:li:dataset:1",
                    "urn:li:dataset:1",
                    "urn:li:dataset:1",
                ],
                "status": ["COMPLETE", "COMPLETE", "COMPLETE"],
            }
        )

        result = list_assertions(df)
        assert len(result) == 1
        assert result[0].assertion_urn == "urn:li:assertion:1"
        assert result[0].assertee_urn == "urn:li:dataset:1"
        assert result[0].point_count == 3

    def test_multiple_assertions(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "assertionUrn": [
                    "urn:li:assertion:1",
                    "urn:li:assertion:1",
                    "urn:li:assertion:2",
                    "urn:li:assertion:2",
                ],
                "status": ["COMPLETE"] * 4,
            }
        )

        result = list_assertions(df)
        assert len(result) == 2

        # Should be sorted by point_count descending
        urns = [r.assertion_urn for r in result]
        assert "urn:li:assertion:1" in urns
        assert "urn:li:assertion:2" in urns

    def test_filter_by_assertion_type(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": [
                    "urn:li:assertion:1",
                    "urn:li:assertion:2",
                    "urn:li:assertion:3",
                ],
                "event_result_assertion_type": ["FIELD", "VOLUME", "FIELD"],
            }
        )

        result = list_assertions(df, assertion_type="FIELD")
        assert len(result) == 2

    def test_timestamp_range_extraction(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
            }
        )

        result = list_assertions(df)
        assert len(result) == 1
        assert result[0].first_event is not None
        assert result[0].last_event is not None
        assert result[0].first_event < result[0].last_event

    def test_value_statistics(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_nativeResults_Metric_Value": [1.0, 5.0, 10.0],
            }
        )

        result = list_assertions(df)
        assert len(result) == 1
        assert result[0].value_min == 1.0
        assert result[0].value_max == 10.0
        assert result[0].value_mean == pytest.approx(5.333, rel=0.01)

    def test_skips_nan_urns(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1", None, "urn:li:assertion:1"],
            }
        )

        result = list_assertions(df)
        assert len(result) == 1
        assert result[0].point_count == 2  # Only non-null URNs counted


class TestExtractTimeseries:
    """Tests for extract_timeseries function."""

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = extract_timeseries(df, "urn:li:assertion:1")
        assert len(result) == 0
        assert list(result.columns) == ["ds", "y"]

    def test_none_dataframe(self):
        result = extract_timeseries(None, "urn:li:assertion:1")  # type: ignore[arg-type]
        assert len(result) == 0
        assert list(result.columns) == ["ds", "y"]

    def test_missing_assertion_urn_column(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000],
                "value": [5.0],
            }
        )
        result = extract_timeseries(df, "urn:li:assertion:1")
        assert len(result) == 0

    def test_assertion_not_found(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000],
                "assertionUrn": ["urn:li:assertion:other"],
                "event_result_nativeResults_Metric_Value": [5.0],
            }
        )
        result = extract_timeseries(df, "urn:li:assertion:1")
        assert len(result) == 0

    def test_basic_extraction(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1")
        assert len(result) == 3
        assert "ds" in result.columns
        assert "y" in result.columns
        assert list(result["y"]) == [1.0, 2.0, 3.0]

    def test_sorted_by_timestamp(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [3000, 1000, 2000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_nativeResults_Metric_Value": [3.0, 1.0, 2.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1")
        assert list(result["y"]) == [1.0, 2.0, 3.0]

    def test_custom_value_column(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000],
                "assertionUrn": ["urn:li:assertion:1"] * 2,
                "custom_value": [10.0, 20.0],
            }
        )

        result = extract_timeseries(
            df, "urn:li:assertion:1", value_column="custom_value"
        )
        assert len(result) == 2
        assert list(result["y"]) == [10.0, 20.0]

    def test_filters_nan_values(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_nativeResults_Metric_Value": [1.0, None, 3.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1")
        assert len(result) == 2

    def test_auto_detect_value_column_by_type(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000],
                "assertionUrn": ["urn:li:assertion:1"],
                "event_result_assertion_type": ["FIELD"],
                "event_result_nativeResults_Metric_Value": [5.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1")
        assert len(result) == 1
        assert result["y"].iloc[0] == 5.0

    def test_includes_all_result_types_by_default(self):
        """By default, all result types should be included (for browsing)."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "assertionUrn": ["urn:li:assertion:1"] * 4,
                "event_result_result_type": ["SUCCESS", "FAILURE", "ERROR", "SUCCESS"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1")
        assert len(result) == 4
        assert list(result["y"]) == [1.0, 2.0, 3.0, 4.0]

    def test_exclude_errors_keeps_success_and_init(self):
        """With exclude_errors=True, FAILURE/ERROR are excluded but SUCCESS/INIT remain."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000, 5000],
                "assertionUrn": ["urn:li:assertion:1"] * 5,
                "event_result_result_type": [
                    "SUCCESS",
                    "FAILURE",
                    "ERROR",
                    "INIT",
                    "SUCCESS",
                ],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1", exclude_errors=True)
        assert len(result) == 3
        assert list(result["y"]) == [1.0, 4.0, 5.0]

    def test_exclude_errors_and_exclude_init_combined(self):
        """exclude_errors + exclude_init keeps only SUCCESS (like old filter_success_only)."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000, 5000],
                "assertionUrn": ["urn:li:assertion:1"] * 5,
                "event_result_result_type": [
                    "SUCCESS",
                    "FAILURE",
                    "ERROR",
                    "INIT",
                    "SUCCESS",
                ],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )

        result = extract_timeseries(
            df, "urn:li:assertion:1", exclude_errors=True, exclude_init=True
        )
        assert len(result) == 2
        assert list(result["y"]) == [1.0, 5.0]

    def test_exclude_init_excludes_only_init_results(self):
        """With exclude_init=True alone, only INIT is excluded (others remain)."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "assertionUrn": ["urn:li:assertion:1"] * 4,
                "event_result_result_type": ["INIT", "SUCCESS", "FAILURE", "INIT"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1", exclude_init=True)
        assert len(result) == 2
        assert list(result["y"]) == [2.0, 3.0]

    def test_includes_all_when_no_result_type_column(self):
        """When event_result_result_type column is missing, include all events."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0],
            }
        )

        # Even with exclude_errors=True, if column doesn't exist, include all
        result = extract_timeseries(df, "urn:li:assertion:1", exclude_errors=True)
        assert len(result) == 3
        assert list(result["y"]) == [1.0, 2.0, 3.0]

    def test_returns_empty_when_all_excluded(self):
        """Should return empty DataFrame if all results are excluded."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000],
                "assertionUrn": ["urn:li:assertion:1"] * 2,
                "event_result_result_type": ["FAILURE", "ERROR"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1", exclude_errors=True)
        assert len(result) == 0
        assert list(result.columns) == ["ds", "y"]

    def test_exclude_init_default_is_false(self):
        """By default, INIT results are included."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_result_type": ["INIT", "SUCCESS", "INIT"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1")
        assert len(result) == 3
        assert list(result["y"]) == [1.0, 2.0, 3.0]

    def test_exclude_init_returns_empty_when_all_init(self):
        """Should return empty DataFrame if all results are INIT when excluding."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000],
                "assertionUrn": ["urn:li:assertion:1"] * 2,
                "event_result_result_type": ["INIT", "INIT"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1", exclude_init=True)
        assert len(result) == 0
        assert list(result.columns) == ["ds", "y"]

    def test_preprocessing_scenario_include_init(self):
        """Preprocessing with INIT included: exclude_errors=True, exclude_init=False."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "assertionUrn": ["urn:li:assertion:1"] * 4,
                "event_result_result_type": ["SUCCESS", "INIT", "FAILURE", "SUCCESS"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries(
            df, "urn:li:assertion:1", exclude_errors=True, exclude_init=False
        )
        assert len(result) == 3
        assert list(result["y"]) == [1.0, 2.0, 4.0]

    def test_preprocessing_scenario_exclude_init(self):
        """Preprocessing with INIT excluded: exclude_errors=True, exclude_init=True."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "assertionUrn": ["urn:li:assertion:1"] * 4,
                "event_result_result_type": ["SUCCESS", "INIT", "FAILURE", "SUCCESS"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries(
            df, "urn:li:assertion:1", exclude_errors=True, exclude_init=True
        )
        assert len(result) == 2
        assert list(result["y"]) == [1.0, 4.0]

    def test_include_type_column_adds_type(self):
        """include_type_column=True adds 'type' column with result types."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_result_type": ["INIT", "SUCCESS", "FAILURE"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0],
            }
        )

        result = extract_timeseries(df, "urn:li:assertion:1", include_type_column=True)
        assert len(result) == 3
        assert "type" in result.columns
        assert list(result["type"]) == ["INIT", "SUCCESS", "FAILURE"]

    def test_include_type_column_ignores_exclude_init(self):
        """When include_type_column=True, exclude_init is ignored (delegated to preprocessing)."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_result_type": ["INIT", "SUCCESS", "INIT"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0],
            }
        )

        # Even with exclude_init=True, INIT should be included when type column is requested
        result = extract_timeseries(
            df, "urn:li:assertion:1", exclude_init=True, include_type_column=True
        )
        assert len(result) == 3
        assert list(result["type"]) == ["INIT", "SUCCESS", "INIT"]

    def test_include_type_column_respects_exclude_errors(self):
        """exclude_errors still works when include_type_column=True."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "assertionUrn": ["urn:li:assertion:1"] * 4,
                "event_result_result_type": ["INIT", "SUCCESS", "FAILURE", "ERROR"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries(
            df, "urn:li:assertion:1", exclude_errors=True, include_type_column=True
        )
        assert len(result) == 2
        assert list(result["type"]) == ["INIT", "SUCCESS"]

    def test_include_type_column_empty_returns_correct_columns(self):
        """Empty result with include_type_column=True has correct columns."""
        df = pd.DataFrame()
        result = extract_timeseries(df, "urn:li:assertion:1", include_type_column=True)
        assert len(result) == 0
        assert list(result.columns) == ["ds", "y", "type"]


class TestExtractTimeseriesWithBounds:
    """Tests for extract_timeseries_with_bounds function."""

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = extract_timeseries_with_bounds(df, "urn:li:assertion:1")
        assert len(result) == 0
        assert list(result.columns) == ["ds", "y", "y_min", "y_max"]

    def test_with_all_bounds(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000],
                "assertionUrn": ["urn:li:assertion:1"] * 2,
                "event_result_nativeResults_Metric_Value": [5.0, 6.0],
                "event_result_nativeResults_Compared_Min_Value": [3.0, 4.0],
                "event_result_nativeResults_Compared_Max_Value": [7.0, 8.0],
            }
        )

        result = extract_timeseries_with_bounds(df, "urn:li:assertion:1")
        assert len(result) == 2
        assert list(result["y"]) == [5.0, 6.0]
        assert list(result["y_min"]) == [3.0, 4.0]
        assert list(result["y_max"]) == [7.0, 8.0]

    def test_missing_bounds(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000],
                "assertionUrn": ["urn:li:assertion:1"],
                "event_result_nativeResults_Metric_Value": [5.0],
            }
        )

        result = extract_timeseries_with_bounds(df, "urn:li:assertion:1")
        assert len(result) == 1
        assert result["y"].iloc[0] == 5.0
        # y_min and y_max should be NA
        assert pd.isna(result["y_min"].iloc[0])
        assert pd.isna(result["y_max"].iloc[0])

    def test_includes_all_result_types_by_default(self):
        """By default, all result types should be included (for browsing)."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_result_type": ["SUCCESS", "FAILURE", "SUCCESS"],
                "event_result_nativeResults_Metric_Value": [5.0, 6.0, 7.0],
                "event_result_nativeResults_Compared_Min_Value": [3.0, 4.0, 5.0],
                "event_result_nativeResults_Compared_Max_Value": [7.0, 8.0, 9.0],
            }
        )

        result = extract_timeseries_with_bounds(df, "urn:li:assertion:1")
        assert len(result) == 3
        assert list(result["y"]) == [5.0, 6.0, 7.0]
        assert list(result["y_min"]) == [3.0, 4.0, 5.0]
        assert list(result["y_max"]) == [7.0, 8.0, 9.0]

    def test_exclude_errors_keeps_success_and_init(self):
        """With exclude_errors=True, FAILURE/ERROR are excluded but SUCCESS/INIT remain."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_result_type": ["SUCCESS", "FAILURE", "SUCCESS"],
                "event_result_nativeResults_Metric_Value": [5.0, 6.0, 7.0],
                "event_result_nativeResults_Compared_Min_Value": [3.0, 4.0, 5.0],
                "event_result_nativeResults_Compared_Max_Value": [7.0, 8.0, 9.0],
            }
        )

        result = extract_timeseries_with_bounds(
            df, "urn:li:assertion:1", exclude_errors=True
        )
        assert len(result) == 2
        assert list(result["y"]) == [5.0, 7.0]
        assert list(result["y_min"]) == [3.0, 5.0]
        assert list(result["y_max"]) == [7.0, 9.0]

    def test_exclude_errors_and_exclude_init_combined(self):
        """exclude_errors + exclude_init keeps only SUCCESS."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "assertionUrn": ["urn:li:assertion:1"] * 4,
                "event_result_result_type": ["ERROR", "SUCCESS", "INIT", "FAILURE"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries_with_bounds(
            df, "urn:li:assertion:1", exclude_errors=True, exclude_init=True
        )
        assert len(result) == 1
        assert result["y"].iloc[0] == 2.0

    def test_includes_all_when_no_result_type_column(self):
        """When event_result_result_type column is missing, include all events."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000],
                "assertionUrn": ["urn:li:assertion:1"] * 2,
                "event_result_nativeResults_Metric_Value": [5.0, 6.0],
            }
        )

        # Even with exclude_errors=True, if column doesn't exist, include all
        result = extract_timeseries_with_bounds(
            df, "urn:li:assertion:1", exclude_errors=True
        )
        assert len(result) == 2
        assert list(result["y"]) == [5.0, 6.0]

    def test_exclude_init_excludes_only_init_results(self):
        """With exclude_init=True, INIT results are excluded but others remain."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_result_type": ["INIT", "SUCCESS", "FAILURE"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0],
                "event_result_nativeResults_Compared_Min_Value": [0.0, 1.0, 2.0],
                "event_result_nativeResults_Compared_Max_Value": [2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries_with_bounds(
            df, "urn:li:assertion:1", exclude_init=True
        )
        assert len(result) == 2
        assert list(result["y"]) == [2.0, 3.0]

    def test_preprocessing_scenario_with_init(self):
        """Preprocessing keeps INIT with exclude_errors=True, exclude_init=False."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "assertionUrn": ["urn:li:assertion:1"] * 4,
                "event_result_result_type": ["INIT", "SUCCESS", "FAILURE", "SUCCESS"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries_with_bounds(
            df, "urn:li:assertion:1", exclude_errors=True, exclude_init=False
        )
        # Should keep INIT and both SUCCESS, exclude FAILURE
        assert len(result) == 3
        assert list(result["y"]) == [1.0, 2.0, 4.0]

    def test_include_type_column_adds_type(self):
        """include_type_column=True adds 'type' column with result types."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_result_type": ["INIT", "SUCCESS", "FAILURE"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0],
                "event_result_nativeResults_Compared_Min_Value": [0.0, 1.0, 2.0],
                "event_result_nativeResults_Compared_Max_Value": [2.0, 3.0, 4.0],
            }
        )

        result = extract_timeseries_with_bounds(
            df, "urn:li:assertion:1", include_type_column=True
        )
        assert len(result) == 3
        assert "type" in result.columns
        assert list(result["type"]) == ["INIT", "SUCCESS", "FAILURE"]

    def test_include_type_column_ignores_exclude_init(self):
        """When include_type_column=True, exclude_init is ignored."""
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "event_result_result_type": ["INIT", "SUCCESS", "INIT"],
                "event_result_nativeResults_Metric_Value": [1.0, 2.0, 3.0],
            }
        )

        # Even with exclude_init=True, INIT should be included when type column is requested
        result = extract_timeseries_with_bounds(
            df, "urn:li:assertion:1", exclude_init=True, include_type_column=True
        )
        assert len(result) == 3
        assert list(result["type"]) == ["INIT", "SUCCESS", "INIT"]

    def test_include_type_column_empty_returns_correct_columns(self):
        """Empty result with include_type_column=True has correct columns."""
        df = pd.DataFrame()
        result = extract_timeseries_with_bounds(
            df, "urn:li:assertion:1", include_type_column=True
        )
        assert len(result) == 0
        assert list(result.columns) == ["ds", "y", "y_min", "y_max", "type"]


class TestFilterEventsByTime:
    """Tests for filter_events_by_time function."""

    def test_none_dataframe(self):
        result = filter_events_by_time(None)  # type: ignore[arg-type]
        assert result is None

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = filter_events_by_time(df)
        assert len(result) == 0

    def test_no_filters(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "value": [1, 2, 3],
            }
        )
        result = filter_events_by_time(df)
        assert len(result) == 3

    def test_start_time_filter(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "value": [1, 2, 3],
            }
        )
        start = datetime.fromtimestamp(2.0, tz=timezone.utc)  # 2000ms
        result = filter_events_by_time(df, start_time=start)
        assert len(result) == 2

    def test_end_time_filter(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "value": [1, 2, 3],
            }
        )
        end = datetime.fromtimestamp(2.0, tz=timezone.utc)  # 2000ms
        result = filter_events_by_time(df, end_time=end)
        assert len(result) == 2

    def test_both_filters(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "value": [1, 2, 3, 4],
            }
        )
        start = datetime.fromtimestamp(1.5, tz=timezone.utc)
        end = datetime.fromtimestamp(3.5, tz=timezone.utc)
        result = filter_events_by_time(df, start_time=start, end_time=end)
        assert len(result) == 2


class TestGetTimeRange:
    """Tests for get_time_range function."""

    def test_none_dataframe(self):
        start, end = get_time_range(None)  # type: ignore[arg-type]
        assert start is None
        assert end is None

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        start, end = get_time_range(df)
        assert start is None
        assert end is None

    def test_missing_timestamp_column(self):
        df = pd.DataFrame({"value": [1, 2, 3]})
        start, end = get_time_range(df)
        assert start is None
        assert end is None

    def test_valid_time_range(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
            }
        )
        start, end = get_time_range(df)
        assert start is not None
        assert end is not None
        assert start < end

    def test_handles_nan_timestamps(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, None, 3000],
            }
        )
        start, end = get_time_range(df)
        assert start is not None
        assert end is not None


class TestGetAssertionTypes:
    """Tests for get_assertion_types function."""

    def test_none_dataframe(self):
        result = get_assertion_types(None)  # type: ignore[arg-type]
        assert result == {}

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = get_assertion_types(df)
        assert result == {}

    def test_missing_type_column(self):
        df = pd.DataFrame({"assertionUrn": ["urn:li:assertion:1"]})
        result = get_assertion_types(df)
        assert result == {}

    def test_count_types(self):
        df = pd.DataFrame(
            {
                "event_result_assertion_type": [
                    "FIELD",
                    "FIELD",
                    "VOLUME",
                    "FRESHNESS",
                ],
            }
        )
        result = get_assertion_types(df)
        assert result["FIELD"] == 2
        assert result["VOLUME"] == 1
        assert result["FRESHNESS"] == 1


class TestGetResultTypeCounts:
    """Tests for get_result_type_counts function."""

    def test_none_dataframe(self):
        result = get_result_type_counts(None)  # type: ignore[arg-type]
        assert result == {}

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = get_result_type_counts(df)
        assert result == {}

    def test_missing_result_type_column(self):
        df = pd.DataFrame({"assertionUrn": ["urn:li:assertion:1"]})
        result = get_result_type_counts(df)
        assert result == {}

    def test_count_result_types(self):
        df = pd.DataFrame(
            {
                "event_result_result_type": [
                    "SUCCESS",
                    "SUCCESS",
                    "FAILURE",
                    "ERROR",
                ],
            }
        )
        result = get_result_type_counts(df)
        assert result["SUCCESS"] == 2
        assert result["FAILURE"] == 1
        assert result["ERROR"] == 1

    def test_filter_by_assertion_urn(self):
        df = pd.DataFrame(
            {
                "assertionUrn": [
                    "urn:li:assertion:1",
                    "urn:li:assertion:1",
                    "urn:li:assertion:2",
                    "urn:li:assertion:2",
                ],
                "event_result_result_type": ["SUCCESS", "FAILURE", "SUCCESS", "ERROR"],
            }
        )
        result = get_result_type_counts(df, assertion_urn="urn:li:assertion:1")
        assert result["SUCCESS"] == 1
        assert result["FAILURE"] == 1
        assert "ERROR" not in result


class TestGetAssertionMetadata:
    """Tests for get_assertion_metadata function."""

    def test_none_dataframe(self):
        result = get_assertion_metadata(None, "urn:li:assertion:1")  # type: ignore[arg-type]
        assert result is None

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = get_assertion_metadata(df, "urn:li:assertion:1")
        assert result is None

    def test_assertion_not_found(self):
        df = pd.DataFrame(
            {
                "assertionUrn": ["urn:li:assertion:other"],
                "timestampMillis": [1000],
            }
        )
        result = get_assertion_metadata(df, "urn:li:assertion:1")
        assert result is None

    def test_assertion_found(self):
        df = pd.DataFrame(
            {
                "assertionUrn": ["urn:li:assertion:1"] * 3,
                "timestampMillis": [1000, 2000, 3000],
                "asserteeUrn": ["urn:li:dataset:1"] * 3,
            }
        )
        result = get_assertion_metadata(df, "urn:li:assertion:1")
        assert result is not None
        assert result.assertion_urn == "urn:li:assertion:1"
        assert result.point_count == 3


class TestConstants:
    """Tests for module constants."""

    def test_timestamp_column(self):
        assert TIMESTAMP_COLUMN == "timestampMillis"

    def test_assertion_type_columns(self):
        assert "FIELD" in ASSERTION_TYPE_VALUE_COLUMNS
        assert "VOLUME" in ASSERTION_TYPE_VALUE_COLUMNS
        assert "FRESHNESS" in ASSERTION_TYPE_VALUE_COLUMNS

    def test_metric_value_columns(self):
        assert len(METRIC_VALUE_COLUMNS) > 0
        assert "event_result_nativeResults_Metric_Value" in METRIC_VALUE_COLUMNS


class TestExtractEntityFromMonitorUrn:
    """Tests for extract_entity_from_monitor_urn function."""

    def test_extracts_dataset_urn_from_monitor_urn(self):
        monitor_urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset2-v2,PROD),__system__volume)"
        result = extract_entity_from_monitor_urn(monitor_urn)
        assert (
            result
            == "urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset2-v2,PROD)"
        )

    def test_extracts_simple_entity_urn(self):
        monitor_urn = "urn:li:monitor:(urn:li:dataset:test,my_monitor)"
        result = extract_entity_from_monitor_urn(monitor_urn)
        assert result == "urn:li:dataset:test"

    def test_returns_none_for_invalid_urn(self):
        assert extract_entity_from_monitor_urn(None) is None
        assert extract_entity_from_monitor_urn("") is None
        assert extract_entity_from_monitor_urn("not-a-urn") is None
        assert extract_entity_from_monitor_urn("urn:li:assertion:123") is None

    def test_returns_none_for_malformed_monitor_urn(self):
        # Missing parentheses
        assert extract_entity_from_monitor_urn("urn:li:monitor:") is None
        # Missing closing paren
        assert extract_entity_from_monitor_urn("urn:li:monitor:(test") is None
        # No comma separator
        assert extract_entity_from_monitor_urn("urn:li:monitor:(no_comma)") is None

    def test_handles_complex_nested_urns(self):
        # Complex nested URN with multiple commas in entity
        monitor_urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),__system__freshness)"
        result = extract_entity_from_monitor_urn(monitor_urn)
        assert (
            result
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )


class TestListMonitors:
    """Tests for list_monitors function."""

    def test_empty_dataframe(self):
        """Test with empty DataFrame."""
        result = list_monitors(pd.DataFrame())
        assert result == []

    def test_none_dataframe(self):
        """Test with None DataFrame."""
        result = list_monitors(None)  # type: ignore[arg-type]
        assert result == []

    def test_missing_monitor_urn_column(self):
        """Test DataFrame without monitorUrn column."""
        df = pd.DataFrame({"timestampMillis": [1000]})
        result = list_monitors(df)
        assert result == []

    def test_basic_monitor_listing(self):
        """Test basic monitor listing."""
        df = pd.DataFrame(
            {
                "monitorUrn": [
                    "urn:li:monitor:(urn:li:dataset:test,__system__volume)",
                    "urn:li:monitor:(urn:li:dataset:test,__system__volume)",
                ],
                "timestampMillis": [1704067200000, 1704153600000],
                "state": ["NORMAL", "ANOMALY"],
            }
        )

        result = list_monitors(df)

        assert len(result) == 1
        assert isinstance(result[0], MonitorMetadata)
        assert (
            result[0].monitor_urn
            == "urn:li:monitor:(urn:li:dataset:test,__system__volume)"
        )
        assert result[0].entity_urn == "urn:li:dataset:test"
        assert result[0].point_count == 2

    def test_extracts_entity_urn_from_monitor_urn(self):
        """Test entity URN is extracted from monitor URN structure."""
        df = pd.DataFrame(
            {
                "monitorUrn": [
                    "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:kafka,my-dataset,PROD),__system__volume)"
                ],
                "timestampMillis": [1704067200000],
            }
        )

        result = list_monitors(df)

        assert len(result) == 1
        assert (
            result[0].entity_urn
            == "urn:li:dataset:(urn:li:dataPlatform:kafka,my-dataset,PROD)"
        )

    def test_event_type_anomaly(self):
        """Test event_type is 'anomaly' for monitorAnomalyEvent aspect."""
        df = pd.DataFrame(
            {
                "monitorUrn": ["urn:li:monitor:(urn:li:dataset:test,__system__)"],
                "timestampMillis": [1704067200000],
                "state": ["ANOMALY"],
            }
        )

        result = list_monitors(df, aspect_name="monitorAnomalyEvent")

        assert result[0].event_type == "anomaly"

    def test_event_type_state(self):
        """Test event_type is 'state' for monitorTimeseriesState aspect."""
        df = pd.DataFrame(
            {
                "monitorUrn": ["urn:li:monitor:(urn:li:dataset:test,__system__)"],
                "timestampMillis": [1704067200000],
            }
        )

        result = list_monitors(df, aspect_name="monitorTimeseriesState")

        assert result[0].event_type == "state"

    def test_counts_anomalies(self):
        """Test anomaly counting."""
        df = pd.DataFrame(
            {
                "monitorUrn": ["urn:li:monitor:(urn:li:dataset:test,m)"] * 5,
                "timestampMillis": [1000, 2000, 3000, 4000, 5000],
                "state": ["ANOMALY", "NORMAL", "ANOMALY", "ANOMALY", "NORMAL"],
            }
        )

        result = list_monitors(df, aspect_name="monitorAnomalyEvent")

        assert result[0].anomaly_count == 3

    def test_collects_state_values(self):
        """Test state values collection."""
        df = pd.DataFrame(
            {
                "monitorUrn": ["urn:li:monitor:(urn:li:dataset:test,m)"] * 3,
                "timestampMillis": [1000, 2000, 3000],
                "state": ["CONFIRMED", "REJECTED", "CONFIRMED"],
            }
        )

        result = list_monitors(df)

        assert result[0].state_values is not None
        assert set(result[0].state_values) == {"CONFIRMED", "REJECTED"}

    def test_timestamp_range(self):
        """Test first_event and last_event timestamps."""
        df = pd.DataFrame(
            {
                "monitorUrn": ["urn:li:monitor:(urn:li:dataset:test,m)"] * 3,
                "timestampMillis": [1704067200000, 1704153600000, 1704240000000],
            }
        )

        result = list_monitors(df)

        assert result[0].first_event is not None
        assert result[0].last_event is not None
        # Check that first < last
        assert result[0].first_event < result[0].last_event

    def test_multiple_monitors_sorted_by_point_count(self):
        """Test multiple monitors are sorted by point count (descending)."""
        df = pd.DataFrame(
            {
                "monitorUrn": [
                    "urn:li:monitor:(urn:li:dataset:a,m)",  # 1 event
                    "urn:li:monitor:(urn:li:dataset:b,m)",  # 3 events
                    "urn:li:monitor:(urn:li:dataset:b,m)",
                    "urn:li:monitor:(urn:li:dataset:b,m)",
                    "urn:li:monitor:(urn:li:dataset:c,m)",  # 2 events
                    "urn:li:monitor:(urn:li:dataset:c,m)",
                ],
                "timestampMillis": [1000, 1000, 2000, 3000, 1000, 2000],
            }
        )

        result = list_monitors(df)

        assert len(result) == 3
        assert result[0].point_count == 3  # Monitor b first
        assert result[1].point_count == 2  # Monitor c second
        assert result[2].point_count == 1  # Monitor a last

    def test_skips_nan_monitor_urns(self):
        """Test that NaN monitor URNs are skipped."""
        df = pd.DataFrame(
            {
                "monitorUrn": [
                    "urn:li:monitor:(urn:li:dataset:test,m)",
                    None,
                    pd.NA,
                ],
                "timestampMillis": [1000, 2000, 3000],
            }
        )

        result = list_monitors(df)

        assert len(result) == 1
        assert result[0].monitor_urn == "urn:li:monitor:(urn:li:dataset:test,m)"
