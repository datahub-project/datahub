"""Tests for the assertion_browser.py time series viewer functionality.

Note: These tests were originally for timeseries_viewer.py but that module
has been consolidated into assertion_browser.py.
"""

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

# Note: TestLookupMonitorForAssertion was removed as the
# _lookup_monitor_for_assertion function has been deprecated.
# The monitor URN is now obtained directly from MonitoredAssertionMetadata
# in the monitor-centric architecture.


# Mock dataclasses for testing
@dataclass
class MockAnomalyEdit:
    """Mock AnomalyEdit for testing."""

    monitor_urn: str
    assertion_urn: str
    timestamp_ms: int
    original_state: Optional[str]
    local_state: str
    edited_at: str = "2024-01-01T00:00:00Z"
    is_new: bool = True
    run_event_timestamp_ms: Optional[int] = None


@dataclass
class MockEnvConfig:
    """Mock environment config for testing."""

    server: str = "http://localhost:8080"
    token: str = "test-token"
    display_name: str = "Test"


class TestPublishAnomalies:
    """Tests for _publish_anomalies function."""

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.assertion_browser.get_active_config"
    )
    @patch("streamlit.error")
    def test_no_config_shows_error(self, mock_st_error, mock_get_config):
        """Test that missing config shows error message."""
        from scripts.streamlit_explorer.timeseries_explorer.assertion_browser import (
            _publish_anomalies,
        )

        mock_get_config.return_value = None

        with tempfile.TemporaryDirectory() as tmpdir:
            from scripts.streamlit_explorer.common.cache_manager import (
                AnomalyEditTracker,
            )

            tracker = AnomalyEditTracker(Path(tmpdir))

            _publish_anomalies(
                hostname="localhost",
                monitor_urn="urn:li:monitor:test",
                edit_tracker=tracker,
                pending_anomalies=[],
            )

        mock_st_error.assert_called_once()
        assert "No API configuration" in mock_st_error.call_args[0][0]

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.assertion_browser.get_active_config"
    )
    @patch("streamlit.warning")
    def test_no_anomalies_shows_warning(self, mock_st_warning, mock_get_config):
        """Test that empty anomalies list shows warning."""
        from scripts.streamlit_explorer.timeseries_explorer.assertion_browser import (
            _publish_anomalies,
        )

        mock_get_config.return_value = MockEnvConfig()

        with tempfile.TemporaryDirectory() as tmpdir:
            from scripts.streamlit_explorer.common.cache_manager import (
                AnomalyEditTracker,
            )

            tracker = AnomalyEditTracker(Path(tmpdir))

            _publish_anomalies(
                hostname="localhost",
                monitor_urn="urn:li:monitor:test",
                edit_tracker=tracker,
                pending_anomalies=[],
            )

        mock_st_warning.assert_called_once()
        assert "No anomalies to publish" in mock_st_warning.call_args[0][0]

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.assertion_browser.get_active_config"
    )
    @patch("streamlit.warning")
    def test_no_monitor_shows_warning(self, mock_st_warning, mock_get_config):
        """Test that missing monitor URN shows warning."""
        from scripts.streamlit_explorer.timeseries_explorer.assertion_browser import (
            _publish_anomalies,
        )

        mock_get_config.return_value = MockEnvConfig()

        anomaly = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state=None,
            local_state="CONFIRMED",
            run_event_timestamp_ms=1000,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            from scripts.streamlit_explorer.common.cache_manager import (
                AnomalyEditTracker,
            )

            tracker = AnomalyEditTracker(Path(tmpdir))

            _publish_anomalies(
                hostname="localhost",
                monitor_urn=None,  # No monitor
                edit_tracker=tracker,
                pending_anomalies=[anomaly],  # type: ignore[list-item]
            )

        mock_st_warning.assert_called_once()
        assert "No anomalies to publish" in mock_st_warning.call_args[0][0]

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.assertion_browser.get_active_config"
    )
    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._create_anomaly_event_rest"
    )
    @patch("scripts.streamlit_explorer.common.cache_manager.EndpointCache")
    @patch("streamlit.spinner")
    @patch("streamlit.success")
    @patch("streamlit.rerun")
    def test_successful_publish(
        self,
        mock_rerun,
        mock_st_success,
        mock_spinner,
        mock_cache,
        mock_create,
        mock_get_config,
    ):
        """Test successful anomaly publish flow."""
        from scripts.streamlit_explorer.timeseries_explorer.assertion_browser import (
            _publish_anomalies,
        )

        mock_get_config.return_value = MockEnvConfig()
        mock_create.return_value = (True, None)
        mock_spinner.return_value.__enter__ = MagicMock()
        mock_spinner.return_value.__exit__ = MagicMock()
        mock_cache_instance = MagicMock()
        mock_cache.return_value = mock_cache_instance

        anomaly = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state=None,
            local_state="CONFIRMED",
            run_event_timestamp_ms=1000,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            from scripts.streamlit_explorer.common.cache_manager import (
                AnomalyEditTracker,
            )

            tracker = AnomalyEditTracker(Path(tmpdir))
            # Create the anomaly so we can verify removal
            tracker.create_new_anomaly(
                monitor_urn="urn:li:monitor:test",
                assertion_urn="urn:li:assertion:test",
                run_event_timestamp_ms=1000,
            )

            _publish_anomalies(
                hostname="localhost",
                monitor_urn="urn:li:monitor:test",
                edit_tracker=tracker,
                pending_anomalies=[anomaly],  # type: ignore[list-item]
            )

        mock_create.assert_called_once()
        mock_st_success.assert_called_once()
        assert "Published 1" in mock_st_success.call_args[0][0]
        mock_cache_instance.update_anomaly_events_after_publish.assert_called_once()
        mock_rerun.assert_called_once()

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.assertion_browser.get_active_config"
    )
    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._create_anomaly_event_rest"
    )
    @patch("streamlit.spinner")
    @patch("streamlit.error")
    def test_failed_publish_shows_errors(
        self,
        mock_st_error,
        mock_spinner,
        mock_create,
        mock_get_config,
    ):
        """Test that failed publishes show error messages."""
        from scripts.streamlit_explorer.timeseries_explorer.assertion_browser import (
            _publish_anomalies,
        )

        mock_get_config.return_value = MockEnvConfig()
        mock_create.return_value = (False, "API returned 500")
        mock_spinner.return_value.__enter__ = MagicMock()
        mock_spinner.return_value.__exit__ = MagicMock()

        anomaly = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state=None,
            local_state="CONFIRMED",
            run_event_timestamp_ms=1000,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            from scripts.streamlit_explorer.common.cache_manager import (
                AnomalyEditTracker,
            )

            tracker = AnomalyEditTracker(Path(tmpdir))

            _publish_anomalies(
                hostname="localhost",
                monitor_urn="urn:li:monitor:test",
                edit_tracker=tracker,
                pending_anomalies=[anomaly],  # type: ignore[list-item]
            )

        mock_create.assert_called_once()
        mock_st_error.assert_called()
        assert "Failed to publish" in mock_st_error.call_args[0][0]


class TestMaxDisplayEventsConstant:
    """Test that the MAX_DISPLAY_EVENTS constant is properly defined."""

    def test_constant_defined(self):
        """Test that MAX_DISPLAY_EVENTS constant exists and is reasonable."""
        from scripts.streamlit_explorer.timeseries_explorer.assertion_browser import (
            MAX_DISPLAY_EVENTS,
        )

        # Hours in a leap year (366 * 24)
        assert MAX_DISPLAY_EVENTS == 8784
        assert isinstance(MAX_DISPLAY_EVENTS, int)
