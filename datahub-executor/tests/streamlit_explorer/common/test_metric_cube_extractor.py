"""Tests for the metric_cube_extractor module."""

from datetime import datetime, timezone

import pandas as pd

from scripts.streamlit_explorer.common.metric_cube_extractor import (
    MetricCubeMetadata,
    build_metric_cube_urn,
    decode_metric_cube_urn,
    extract_timeseries,
    extract_timeseries_with_anomalies,
    filter_events_by_time,
    get_anomaly_counts,
    get_time_range,
    list_metric_cubes,
)


class TestUrnFunctions:
    """Tests for URN encoding/decoding functions."""

    def test_build_metric_cube_urn(self):
        monitor_urn = "urn:li:monitor:(urn:li:dataset:test,test-monitor)"
        result = build_metric_cube_urn(monitor_urn)

        assert result.startswith("urn:li:dataHubMetricCube:")
        # Should be decodable
        decoded = decode_metric_cube_urn(result)
        assert decoded == monitor_urn

    def test_decode_metric_cube_urn(self):
        monitor_urn = "urn:li:monitor:(urn:li:dataset:test,test-monitor)"
        cube_urn = build_metric_cube_urn(monitor_urn)

        result = decode_metric_cube_urn(cube_urn)
        assert result == monitor_urn

    def test_decode_invalid_urn_returns_none(self):
        assert decode_metric_cube_urn(None) is None  # type: ignore[arg-type]
        assert decode_metric_cube_urn("") is None
        assert decode_metric_cube_urn("urn:li:monitor:test") is None

    def test_decode_malformed_base64_returns_none(self):
        result = decode_metric_cube_urn("urn:li:dataHubMetricCube:not-valid-base64!!!")
        assert result is None


class TestMetricCubeMetadata:
    """Tests for MetricCubeMetadata dataclass."""

    def test_dataclass_creation(self):
        metadata = MetricCubeMetadata(
            metric_cube_urn="urn:li:dataHubMetricCube:abc123",
            monitor_urn="urn:li:monitor:test",
            point_count=100,
            first_event=datetime(2024, 1, 1, tzinfo=timezone.utc),
            last_event=datetime(2024, 1, 31, tzinfo=timezone.utc),
            value_min=0.0,
            value_max=10.0,
            value_mean=5.0,
            anomaly_count=3,
        )

        assert metadata.metric_cube_urn == "urn:li:dataHubMetricCube:abc123"
        assert metadata.point_count == 100
        assert metadata.anomaly_count == 3

    def test_dataclass_with_none_values(self):
        metadata = MetricCubeMetadata(
            metric_cube_urn="urn:li:dataHubMetricCube:abc123",
            monitor_urn=None,
            point_count=0,
            first_event=None,
            last_event=None,
            value_min=None,
            value_max=None,
            value_mean=None,
            anomaly_count=0,
        )

        assert metadata.monitor_urn is None
        assert metadata.point_count == 0


class TestListMetricCubes:
    """Tests for list_metric_cubes function."""

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = list_metric_cubes(df)
        assert result == []

    def test_none_dataframe(self):
        result = list_metric_cubes(None)  # type: ignore[arg-type]
        assert result == []

    def test_single_metric_cube_by_monitor_urn(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "monitorUrn": [
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                ],
                "measure": [10.0, 20.0, 30.0],
            }
        )
        result = list_metric_cubes(df)

        assert len(result) == 1
        assert result[0].monitor_urn == "urn:li:monitor:test"
        assert result[0].point_count == 3

    def test_single_metric_cube_by_metric_cube_urn(self):
        cube_urn = build_metric_cube_urn("urn:li:monitor:test")
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000],
                "metricCubeUrn": [cube_urn, cube_urn],
                "measure": [10.0, 20.0],
            }
        )
        result = list_metric_cubes(df)

        assert len(result) == 1
        assert result[0].metric_cube_urn == cube_urn
        assert result[0].point_count == 2

    def test_multiple_metric_cubes(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "monitorUrn": [
                    "urn:li:monitor:test1",
                    "urn:li:monitor:test1",
                    "urn:li:monitor:test2",
                ],
                "measure": [10.0, 20.0, 30.0],
            }
        )
        result = list_metric_cubes(df)

        # Sorted by point count descending
        assert len(result) == 2
        assert result[0].point_count >= result[1].point_count

    def test_value_statistics(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "monitorUrn": [
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                ],
                "measure": [10.0, 20.0, 30.0],
            }
        )
        result = list_metric_cubes(df)

        assert len(result) == 1
        assert result[0].value_min == 10.0
        assert result[0].value_max == 30.0
        assert result[0].value_mean == 20.0

    def test_anomaly_count(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "monitorUrn": [
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                ],
                "measure": [10.0, 20.0, 30.0],
                "anomaly_state": [None, "CONFIRMED", "REJECTED"],
            }
        )
        result = list_metric_cubes(df)

        assert len(result) == 1
        assert result[0].anomaly_count == 2  # CONFIRMED and REJECTED


class TestExtractTimeseries:
    """Tests for extract_timeseries function."""

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = extract_timeseries(df)
        assert len(result) == 0
        assert "ds" in result.columns
        assert "y" in result.columns

    def test_none_dataframe(self):
        result = extract_timeseries(None)  # type: ignore[arg-type]
        assert len(result) == 0

    def test_basic_extraction(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000000, 2000000, 3000000],
                "monitorUrn": [
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                ],
                "measure": [10.0, 20.0, 30.0],
            }
        )
        result = extract_timeseries(df)

        assert len(result) == 3
        assert "ds" in result.columns
        assert "y" in result.columns
        assert list(result["y"]) == [10.0, 20.0, 30.0]

    def test_extraction_with_anomalies(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000000, 2000000, 3000000],
                "monitorUrn": [
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                ],
                "measure": [10.0, 20.0, 30.0],
                "anomaly_state": [None, "CONFIRMED", None],
            }
        )
        result = extract_timeseries(df, include_anomalies=True)

        assert len(result) == 3
        assert "anomaly_state" in result.columns

    def test_extraction_filters_by_monitor_urn(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000000, 2000000, 3000000],
                "monitorUrn": [
                    "urn:li:monitor:test1",
                    "urn:li:monitor:test1",
                    "urn:li:monitor:test2",
                ],
                "measure": [10.0, 20.0, 30.0],
            }
        )
        result = extract_timeseries(df, monitor_urn="urn:li:monitor:test1")

        assert len(result) == 2
        assert list(result["y"]) == [10.0, 20.0]

    def test_sorted_by_timestamp(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [3000000, 1000000, 2000000],
                "monitorUrn": [
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                ],
                "measure": [30.0, 10.0, 20.0],
            }
        )
        result = extract_timeseries(df)

        # Should be sorted ascending by timestamp
        assert result["ds"].is_monotonic_increasing


class TestExtractTimeseriesWithAnomalies:
    """Tests for extract_timeseries_with_anomalies function."""

    def test_basic_extraction(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000000, 2000000, 3000000],
                "monitorUrn": [
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                ],
                "measure": [10.0, 20.0, 30.0],
                "anomaly_state": [None, "CONFIRMED", "REJECTED"],
            }
        )
        result = extract_timeseries_with_anomalies(df)

        assert len(result) == 3
        assert "is_anomaly" in result.columns
        assert result["is_anomaly"].iloc[0] == False  # noqa: E712
        assert result["is_anomaly"].iloc[1] == True  # noqa: E712


class TestFilterEventsByTime:
    """Tests for filter_events_by_time function."""

    def test_filter_by_start_time(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000000, 2000000, 3000000],
                "measure": [10.0, 20.0, 30.0],
            }
        )
        # 1500000 milliseconds = 1970-01-01 00:25:00 UTC (1500 seconds after epoch)
        start = datetime(1970, 1, 1, 0, 25, 0, tzinfo=timezone.utc)
        result = filter_events_by_time(df, start_time=start)

        # Only timestamps >= 1500000 ms should be included: 2000000, 3000000
        assert len(result) == 2

    def test_filter_by_end_time(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000000, 2000000, 3000000],
                "measure": [10.0, 20.0, 30.0],
            }
        )
        # 2500000 milliseconds = 1970-01-01 00:41:40 UTC (2500 seconds after epoch)
        end = datetime(1970, 1, 1, 0, 41, 40, tzinfo=timezone.utc)
        result = filter_events_by_time(df, end_time=end)

        # Only timestamps <= 2500000 ms should be included: 1000000, 2000000
        assert len(result) == 2


class TestGetTimeRange:
    """Tests for get_time_range function."""

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        start, end = get_time_range(df)
        assert start is None
        assert end is None

    def test_basic_range(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000000, 2000000, 3000000],
                "measure": [10.0, 20.0, 30.0],
            }
        )
        start, end = get_time_range(df)

        assert start is not None
        assert end is not None
        assert start < end


class TestGetAnomalyCounts:
    """Tests for get_anomaly_counts function."""

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = get_anomaly_counts(df)
        assert result == {}

    def test_counts_anomaly_states(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000, 4000],
                "monitorUrn": [
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                    "urn:li:monitor:test",
                ],
                "anomaly_state": ["CONFIRMED", "CONFIRMED", "REJECTED", None],
            }
        )
        result = get_anomaly_counts(df)

        assert result.get("CONFIRMED") == 2
        assert result.get("REJECTED") == 1

    def test_filters_by_monitor_urn(self):
        df = pd.DataFrame(
            {
                "timestampMillis": [1000, 2000, 3000],
                "monitorUrn": [
                    "urn:li:monitor:test1",
                    "urn:li:monitor:test1",
                    "urn:li:monitor:test2",
                ],
                "anomaly_state": ["CONFIRMED", "CONFIRMED", "CONFIRMED"],
            }
        )
        result = get_anomaly_counts(df, monitor_urn="urn:li:monitor:test1")

        assert result.get("CONFIRMED") == 2
