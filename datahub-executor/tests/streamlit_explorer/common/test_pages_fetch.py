"""Tests for conversion functions in common/fetch.py.

These functions are internal to the pages module but critical for correct data handling.
"""

import pandas as pd
import pytest


def get_convert_monitor_anomaly_events():
    """Import the monitor anomaly conversion function."""
    try:
        from scripts.streamlit_explorer.common import _convert_monitor_anomaly_events

        return _convert_monitor_anomaly_events
    except ImportError:
        pytest.skip("Streamlit not available")


class TestConvertMonitorAnomalyEvents:
    """Tests for _convert_monitor_anomaly_events function."""

    def test_empty_events_list(self):
        """Test conversion with empty list."""
        func = get_convert_monitor_anomaly_events()
        result = func([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_basic_anomaly_event(self):
        """Test conversion of basic monitor anomaly event."""
        func = get_convert_monitor_anomaly_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "entityUrn": "urn:li:monitor:123",
                "aspectName": "monitorAnomalyEvent",
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["timestampMillis"].iloc[0] == 1704067200000

    def test_anomaly_event_with_source(self):
        """Test conversion of anomaly event with source details."""
        func = get_convert_monitor_anomaly_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "state": "CONFIRMED",
                "source": {
                    "type": "ASSERTION_RESULT",
                    "sourceUrn": "urn:li:assertion:123",
                    "sourceEventTimestampMillis": 1704000000000,
                    "properties": {
                        "assertionMetric": {
                            "timestampMillis": 1704000000000,
                            "value": 42.5,
                        }
                    },
                },
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["state"].iloc[0] == "CONFIRMED"
        assert result["source_type"].iloc[0] == "ASSERTION_RESULT"
        assert result["source_sourceUrn"].iloc[0] == "urn:li:assertion:123"
        assert result["source_sourceEventTimestampMillis"].iloc[0] == 1704000000000
        assert result["source_assertionMetric_value"].iloc[0] == 42.5

    def test_anomaly_event_uses_datetime_column(self):
        """Test that datetime column is created from source timestamp."""
        func = get_convert_monitor_anomaly_events()

        events = [
            {
                "timestampMillis": 1704067200000,  # Anomaly creation time
                "monitorUrn": "urn:li:monitor:test",
                "source": {
                    "sourceEventTimestampMillis": 1704000000000,  # Original event time
                },
            }
        ]

        result = func(events)

        assert "datetime" in result.columns
        # datetime should use source_sourceEventTimestampMillis
        assert result["source_sourceEventTimestampMillis"].iloc[0] == 1704000000000

    def test_anomaly_event_extracts_assertion_urn(self):
        """Test that assertionUrn is extracted from source.sourceUrn for assertion sources."""
        func = get_convert_monitor_anomaly_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "state": "CONFIRMED",
                "source": {
                    "type": "ASSERTION_RESULT",
                    "sourceUrn": "urn:li:assertion:b504626f-1979-4d65-b7db-b9448c0e3d81",
                    "sourceEventTimestampMillis": 1704000000000,
                },
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert "assertionUrn" in result.columns
        assert (
            result["assertionUrn"].iloc[0]
            == "urn:li:assertion:b504626f-1979-4d65-b7db-b9448c0e3d81"
        )
        # source_sourceUrn should also be populated
        assert (
            result["source_sourceUrn"].iloc[0]
            == "urn:li:assertion:b504626f-1979-4d65-b7db-b9448c0e3d81"
        )

    def test_anomaly_event_no_assertion_urn_for_non_assertion_source(self):
        """Test that assertionUrn is not set when sourceUrn is not an assertion."""
        func = get_convert_monitor_anomaly_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "state": "CONFIRMED",
                "source": {
                    "type": "OTHER",
                    "sourceUrn": "urn:li:dataset:some-dataset",
                    "sourceEventTimestampMillis": 1704000000000,
                },
            }
        ]

        result = func(events)

        assert len(result) == 1
        # assertionUrn column may not exist or should be NaN for non-assertion sources
        if "assertionUrn" in result.columns:
            assert pd.isna(result["assertionUrn"].iloc[0])
        # source_sourceUrn should still be populated
        assert result["source_sourceUrn"].iloc[0] == "urn:li:dataset:some-dataset"

    def test_anomaly_event_no_assertion_urn_when_source_missing(self):
        """Test that assertionUrn is handled gracefully when source is missing."""
        func = get_convert_monitor_anomaly_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "state": "CONFIRMED",
                # No source field
            }
        ]

        result = func(events)

        assert len(result) == 1
        # Should not raise, assertionUrn may not exist or be NaN
        if "assertionUrn" in result.columns:
            assert pd.isna(result["assertionUrn"].iloc[0])


def get_convert_monitor_state_events():
    """Import the monitor state events conversion function."""
    try:
        from scripts.streamlit_explorer.common.fetch import (
            _convert_monitor_state_events,
        )

        return _convert_monitor_state_events
    except ImportError:
        pytest.skip("Streamlit not available")


def get_convert_generic_events():
    """Import the generic events conversion function."""
    try:
        from scripts.streamlit_explorer.common.fetch import (
            _convert_generic_events,
        )

        return _convert_generic_events
    except ImportError:
        pytest.skip("Streamlit not available")


def get_convert_metric_cube_events():
    """Import the metric cube events conversion function."""
    try:
        from scripts.streamlit_explorer.common.fetch import (
            _convert_metric_cube_events,
        )

        return _convert_metric_cube_events
    except ImportError:
        pytest.skip("Streamlit not available")


class TestConvertMonitorStateEvents:
    """Tests for _convert_monitor_state_events function."""

    def test_empty_events_list(self):
        """Test conversion with empty list."""
        func = get_convert_monitor_state_events()
        result = func([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_basic_state_event(self):
        """Test conversion of basic monitor state event."""
        func = get_convert_monitor_state_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "entityUrn": "urn:li:monitor:test",
                "id": "state-001",
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["timestampMillis"].iloc[0] == 1704067200000
        assert result["monitorUrn"].iloc[0] == "urn:li:monitor:test"
        assert result["id"].iloc[0] == "state-001"

    def test_state_event_with_custom_properties(self):
        """Test conversion of state event with custom properties."""
        func = get_convert_monitor_state_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "customProperties": {
                    "status": "HEALTHY",
                    "lastRunTime": "2024-01-01T00:00:00Z",
                },
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["prop_status"].iloc[0] == "HEALTHY"
        assert result["prop_lastRunTime"].iloc[0] == "2024-01-01T00:00:00Z"

    def test_state_event_with_empty_custom_properties(self):
        """Test conversion handles empty or None customProperties."""
        func = get_convert_monitor_state_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "customProperties": None,
            },
            {
                "timestampMillis": 1704153600000,
                "monitorUrn": "urn:li:monitor:test",
                "customProperties": {},
            },
        ]

        result = func(events)

        assert len(result) == 2


class TestConvertGenericEvents:
    """Tests for _convert_generic_events function."""

    def test_empty_events_list(self):
        """Test conversion with empty list."""
        func = get_convert_generic_events()
        result = func([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_flat_event(self):
        """Test conversion of a flat event."""
        func = get_convert_generic_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "entityUrn": "urn:li:entity:test",
                "value": 42,
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["timestampMillis"].iloc[0] == 1704067200000
        assert result["entityUrn"].iloc[0] == "urn:li:entity:test"
        assert result["value"].iloc[0] == 42

    def test_nested_event(self):
        """Test conversion of nested event structure."""
        func = get_convert_generic_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "details": {
                    "type": "TEST",
                    "nested": {
                        "value": 123,
                    },
                },
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["details_type"].iloc[0] == "TEST"
        assert result["details_nested_value"].iloc[0] == 123

    def test_event_with_list_values(self):
        """Test that list values are JSON serialized."""
        func = get_convert_generic_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "tags": ["tag1", "tag2"],
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["tags"].iloc[0] == '["tag1", "tag2"]'


class TestConvertMetricCubeEvents:
    """Tests for _convert_metric_cube_events function."""

    def test_empty_events_list(self):
        """Test conversion with empty list."""
        func = get_convert_metric_cube_events()
        result = func([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_basic_metric_cube_event(self):
        """Test conversion of basic metric cube event."""
        func = get_convert_metric_cube_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "metricCubeUrn": "urn:li:dataHubMetricCube:abc123",
                "measure": 42.5,
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["timestampMillis"].iloc[0] == 1704067200000
        assert result["monitorUrn"].iloc[0] == "urn:li:monitor:test"
        assert result["metricCubeUrn"].iloc[0] == "urn:li:dataHubMetricCube:abc123"
        assert result["measure"].iloc[0] == 42.5
        assert result["aspectName"].iloc[0] == "dataHubMetricCubeEvent"

    def test_metric_cube_event_with_anomaly(self):
        """Test conversion of metric cube event with anomaly data."""
        func = get_convert_metric_cube_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "metricCubeUrn": "urn:li:dataHubMetricCube:abc123",
                "measure": 42.5,
                "anomaly_state": "CONFIRMED",
                "anomaly_timestampMillis": 1704067300000,
                "anomaly_source_type": "THRESHOLD",
                "anomaly_source_urn": "urn:li:assertion:xyz",
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["anomaly_state"].iloc[0] == "CONFIRMED"
        assert result["anomaly_timestampMillis"].iloc[0] == 1704067300000
        assert result["anomaly_source_type"].iloc[0] == "THRESHOLD"
        assert result["anomaly_source_urn"].iloc[0] == "urn:li:assertion:xyz"

    def test_metric_cube_event_has_datetime_column(self):
        """Test that datetime column is created from timestampMillis."""
        func = get_convert_metric_cube_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "metricCubeUrn": "urn:li:dataHubMetricCube:abc123",
                "measure": 42.5,
            }
        ]

        result = func(events)

        assert "datetime" in result.columns
        assert pd.notna(result["datetime"].iloc[0])

    def test_metric_cube_event_with_assertion_urn(self):
        """Test that assertionUrn is included when provided."""
        func = get_convert_metric_cube_events()

        events = [
            {
                "timestampMillis": 1704067200000,
                "monitorUrn": "urn:li:monitor:test",
                "metricCubeUrn": "urn:li:dataHubMetricCube:abc123",
                "assertionUrn": "urn:li:assertion:test-assertion",
                "measure": 42.5,
            }
        ]

        result = func(events)

        assert len(result) == 1
        assert result["assertionUrn"].iloc[0] == "urn:li:assertion:test-assertion"
