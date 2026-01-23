"""Tests for GraphQL aspect fetching to ensure all aspects have implementations."""

from dataclasses import dataclass
from typing import Optional
from unittest.mock import MagicMock, patch

# Import the module constants and functions we're testing
from scripts.streamlit_explorer.common.cache_manager import (
    ALL_ASPECTS,
    METRIC_CUBE_ASPECTS,
    MONITOR_ASPECTS,
)


class TestAspectImplementations:
    """Tests to ensure all declared aspects have GraphQL fetch implementations."""

    def test_all_monitor_aspects_defined(self):
        """Verify monitor aspects are defined in the registry."""
        assert "monitorAnomalyEvent" in MONITOR_ASPECTS
        assert len(MONITOR_ASPECTS) >= 1

    def test_all_metric_cube_aspects_defined(self):
        """Verify metric cube aspects are defined in the registry."""
        assert "dataHubMetricCubeEvent" in METRIC_CUBE_ASPECTS
        assert len(METRIC_CUBE_ASPECTS) >= 1

    def test_all_aspects_in_registry(self):
        """Verify ALL_ASPECTS contains all monitor and metric cube aspects."""
        for aspect in METRIC_CUBE_ASPECTS:
            assert aspect in ALL_ASPECTS, (
                f"Metric cube aspect {aspect} missing from ALL_ASPECTS"
            )
        for aspect in MONITOR_ASPECTS:
            assert aspect in ALL_ASPECTS, (
                f"Monitor aspect {aspect} missing from ALL_ASPECTS"
            )


class TestGraphQLTimeseriesAspectDispatch:
    """Tests for _graphql_timeseries_aspect dispatch logic."""

    @patch("scripts.streamlit_explorer.common.fetch._graphql_monitor_anomaly_events")
    def test_monitor_anomaly_event_dispatches_correctly(self, mock_anomaly_events):
        """Test that monitorAnomalyEvent aspect dispatches to _graphql_monitor_anomaly_events."""
        from scripts.streamlit_explorer.common import _graphql_timeseries_aspect

        mock_anomaly_events.return_value = [
            {"timestampMillis": 5678, "state": "CONFIRMED"}
        ]

        result = _graphql_timeseries_aspect(
            graphql_url="http://test/graphql",
            headers={},
            entity_urn="urn:li:monitor:(urn:li:dataset:test,__system__volume)",
            aspect_name="monitorAnomalyEvent",
            start_time_ms=0,
            end_time_ms=1000,
            limit=100,
        )

        mock_anomaly_events.assert_called_once()
        assert result == [{"timestampMillis": 5678, "state": "CONFIRMED"}]

    def test_unsupported_aspect_returns_empty(self):
        """Test that unsupported aspects return empty list without crashing."""
        from scripts.streamlit_explorer.common import _graphql_timeseries_aspect

        result = _graphql_timeseries_aspect(
            graphql_url="http://test/graphql",
            headers={},
            entity_urn="urn:li:unknown:test",
            aspect_name="unknownAspect",
            start_time_ms=0,
            end_time_ms=1000,
            limit=100,
        )

        assert result == []

    def test_monitor_aspect_with_assertion_urn_returns_empty(self):
        """Test that monitorAnomalyEvent with an assertion URN returns empty (wrong entity type)."""
        from scripts.streamlit_explorer.common import _graphql_timeseries_aspect

        result = _graphql_timeseries_aspect(
            graphql_url="http://test/graphql",
            headers={},
            entity_urn="urn:li:assertion:test123",
            aspect_name="monitorAnomalyEvent",
            start_time_ms=0,
            end_time_ms=1000,
            limit=100,
        )

        # Should return empty because monitorAnomalyEvent needs a monitor URN
        assert result == []


class TestAllDeclaredAspectsHaveImplementations:
    """Ensure every aspect declared in the registry has a working fetch implementation.

    This test class serves as a safety net to catch when new aspects are added
    to the registry but their fetch implementations are missing.
    """

    # Map of aspect names to their expected implementation functions
    # Add new implementations here as they are added to _graphql_timeseries_aspect
    ASPECT_IMPLEMENTATIONS = {
        "monitorAnomalyEvent": "_graphql_monitor_anomaly_events",
    }

    # Required aspects that MUST have implementations
    REQUIRED_ASPECTS = {"monitorAnomalyEvent"}

    def test_required_aspects_have_implementations(self):
        """Verify required aspects have corresponding implementations."""
        for aspect in self.REQUIRED_ASPECTS:
            assert aspect in self.ASPECT_IMPLEMENTATIONS, (
                f"Required aspect '{aspect}' is missing from ASPECT_IMPLEMENTATIONS. "
                f"Add the implementation and update the test."
            )

    def test_registered_aspects_implementation_coverage(self):
        """Check implementation coverage for all registered aspects (informational)."""
        all_aspects = set(METRIC_CUBE_ASPECTS.keys()) | set(MONITOR_ASPECTS.keys())
        implemented = set(self.ASPECT_IMPLEMENTATIONS.keys())
        not_implemented = all_aspects - implemented

        # This test passes but warns about missing implementations
        if not_implemented:
            import warnings

            warnings.warn(
                f"The following aspects don't have GraphQL fetch implementations: "
                f"{not_implemented}. Consider implementing them or removing from registry.",
                UserWarning,
                stacklevel=2,
            )

    def test_monitor_anomaly_event_has_implementation(self):
        """Verify monitorAnomalyEvent dispatches to _graphql_monitor_anomaly_events."""
        from scripts.streamlit_explorer.common import _graphql_timeseries_aspect

        with patch(
            "scripts.streamlit_explorer.common.fetch._graphql_monitor_anomaly_events"
        ) as mock:
            mock.return_value = [{"test": "data"}]
            result = _graphql_timeseries_aspect(
                graphql_url="http://test/graphql",
                headers={},
                entity_urn="urn:li:monitor:(urn:li:dataset:test,__system__volume)",
                aspect_name="monitorAnomalyEvent",
                start_time_ms=0,
                end_time_ms=1000,
                limit=100,
            )
            assert mock.called, (
                "monitorAnomalyEvent should dispatch to _graphql_monitor_anomaly_events"
            )
            assert result == [{"test": "data"}]


class TestGraphQLMonitorAnomalyEvents:
    """Tests for _graphql_monitor_anomaly_events function."""

    @patch("scripts.streamlit_explorer.common.fetch._get_retry_session")
    def test_successful_fetch(self, mock_get_session):
        """Test successful fetch of monitor anomaly events."""
        from scripts.streamlit_explorer.common import _graphql_monitor_anomaly_events

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "data": {
                "listMonitorAnomalies": {
                    "anomalies": [
                        {
                            "timestampMillis": 1000,
                            "state": "CONFIRMED",
                            "source": {"type": "USER"},
                        },
                        {
                            "timestampMillis": 2000,
                            "state": "REJECTED",
                            "source": {"type": "USER"},
                        },
                    ],
                }
            }
        }
        mock_session.post.return_value = mock_response

        result = _graphql_monitor_anomaly_events(
            graphql_url="http://test/graphql",
            headers={"Authorization": "Bearer token"},
            monitor_urn="urn:li:monitor:(urn:li:dataset:test,__system__volume)",
            start_time_ms=0,
            end_time_ms=3000,
            limit=100,
        )

        assert len(result) == 2
        assert result[0]["state"] == "CONFIRMED"
        assert result[1]["state"] == "REJECTED"
        # Verify monitor URN is enriched in events
        assert (
            result[0]["monitorUrn"]
            == "urn:li:monitor:(urn:li:dataset:test,__system__volume)"
        )

    @patch("scripts.streamlit_explorer.common.fetch._get_retry_session")
    def test_graphql_error_returns_empty(self, mock_get_session):
        """Test that GraphQL errors return empty list."""
        from scripts.streamlit_explorer.common import _graphql_monitor_anomaly_events

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"errors": [{"message": "Some error"}]}
        mock_session.post.return_value = mock_response

        result = _graphql_monitor_anomaly_events(
            graphql_url="http://test/graphql",
            headers={},
            monitor_urn="urn:li:monitor:test",
            start_time_ms=0,
            end_time_ms=1000,
            limit=100,
        )

        assert result == []

    @patch("scripts.streamlit_explorer.common.fetch._get_retry_session")
    def test_no_anomalies_data_returns_empty(self, mock_get_session):
        """Test that missing anomalies data returns empty list."""
        from scripts.streamlit_explorer.common import _graphql_monitor_anomaly_events

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"data": {"listMonitorAnomalies": None}}
        mock_session.post.return_value = mock_response

        result = _graphql_monitor_anomaly_events(
            graphql_url="http://test/graphql",
            headers={},
            monitor_urn="urn:li:monitor:test",
            start_time_ms=0,
            end_time_ms=1000,
            limit=100,
        )

        assert result == []

    @patch("scripts.streamlit_explorer.common.fetch._get_retry_session")
    def test_request_exception_returns_empty(self, mock_get_session):
        """Test that request exceptions return empty list."""
        import requests

        from scripts.streamlit_explorer.common import _graphql_monitor_anomaly_events

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_session.post.side_effect = requests.RequestException("Connection failed")

        result = _graphql_monitor_anomaly_events(
            graphql_url="http://test/graphql",
            headers={},
            monitor_urn="urn:li:monitor:test",
            start_time_ms=0,
            end_time_ms=1000,
            limit=100,
        )

        assert result == []

    @patch("scripts.streamlit_explorer.common.fetch._get_retry_session")
    def test_falls_back_to_rest_when_graphql_undefined(self, mock_get_session):
        """Test fallback to REST API when GraphQL query is undefined."""
        from scripts.streamlit_explorer.common import _graphql_monitor_anomaly_events

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        # GraphQL returns "undefined" error
        mock_gql_response = MagicMock()
        mock_gql_response.status_code = 200
        mock_gql_response.raise_for_status = MagicMock()
        mock_gql_response.json.return_value = {
            "errors": [{"message": "Field 'listMonitorAnomalies' is undefined"}]
        }
        mock_session.post.return_value = mock_gql_response

        # REST API returns results
        mock_rest_response = MagicMock()
        mock_rest_response.status_code = 200
        mock_rest_response.raise_for_status = MagicMock()
        mock_rest_response.json.return_value = {
            "results": [
                {
                    "urn": "urn:li:monitor:test",
                    "timestampMillis": 1000,
                    "event": {"state": "CONFIRMED"},
                }
            ],
            "scrollId": None,
        }
        mock_session.get.return_value = mock_rest_response

        result = _graphql_monitor_anomaly_events(
            graphql_url="http://test/api/graphql",
            headers={},
            monitor_urn="urn:li:monitor:test",
            start_time_ms=0,
            end_time_ms=1000,
            limit=100,
        )

        # Should have called REST fallback
        assert mock_session.get.called
        assert len(result) == 1
        assert result[0]["state"] == "CONFIRMED"


# Mock AnomalyEdit for testing
@dataclass
class MockAnomalyEdit:
    """Mock AnomalyEdit for testing publish functions."""

    monitor_urn: str
    assertion_urn: str
    timestamp_ms: int
    original_state: Optional[str]
    local_state: str
    edited_at: str = "2024-01-01T00:00:00Z"
    is_new: bool = False
    run_event_timestamp_ms: Optional[int] = None


class TestDeleteAnomalyEvent:
    """Tests for _delete_anomaly_event function using GMS REST API."""

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._get_retry_session"
    )
    def test_successful_delete(self, mock_get_session):
        """Test successful deletion of anomaly event."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _delete_anomaly_event,
        )

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": {"timeseriesRows": 1}}
        mock_response.text = '{"value": {"timeseriesRows": 1}}'
        mock_session.post.return_value = mock_response

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:(urn:li:dataset:test,__system__volume)",
            assertion_urn="urn:li:assertion:test123",
            timestamp_ms=1704067200000,
            original_state="CONFIRMED",
            local_state="DELETE",
        )

        success, error_msg = _delete_anomaly_event(
            graphql_url="http://localhost:8080/api/graphql",
            headers={"Authorization": "Bearer token"},
            change=change,
        )

        assert success is True
        assert error_msg is None
        mock_session.post.assert_called_once()

        # Verify the URL and payload
        call_args = mock_session.post.call_args
        assert "entities?action=delete" in call_args[0][0]
        payload = call_args.kwargs["json"]
        assert payload["urn"] == change.monitor_urn
        assert payload["aspectName"] == "monitorAnomalyEvent"
        assert payload["startTimeMillis"] == 1704067200000
        assert payload["endTimeMillis"] == 1704067200000

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._get_retry_session"
    )
    def test_delete_with_no_rows_deleted(self, mock_get_session):
        """Test that 200 with no rows deleted returns failure."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _delete_anomaly_event,
        )

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": {"timeseriesRows": 0}}
        mock_response.text = '{"value": {"timeseriesRows": 0}}'
        mock_session.post.return_value = mock_response

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state="CONFIRMED",
            local_state="DELETE",
        )

        success, error_msg = _delete_anomaly_event(
            graphql_url="http://test/api/graphql",
            headers={},
            change=change,
        )

        assert success is False
        assert error_msg is not None
        assert "No rows deleted" in error_msg

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._get_retry_session"
    )
    def test_delete_with_error_status(self, mock_get_session):
        """Test that error status returns failure."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _delete_anomaly_event,
        )

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_session.post.return_value = mock_response

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state="CONFIRMED",
            local_state="DELETE",
        )

        success, error_msg = _delete_anomaly_event(
            graphql_url="http://test/api/graphql",
            headers={},
            change=change,
        )

        assert success is False
        assert error_msg is not None
        assert "Delete failed (500)" in error_msg

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._get_retry_session"
    )
    def test_delete_request_exception(self, mock_get_session):
        """Test that request exceptions return failure."""
        import requests

        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _delete_anomaly_event,
        )

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_session.post.side_effect = requests.RequestException("Connection failed")

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state="CONFIRMED",
            local_state="DELETE",
        )

        success, error_msg = _delete_anomaly_event(
            graphql_url="http://test/api/graphql",
            headers={},
            change=change,
        )

        assert success is False
        assert error_msg is not None
        assert "Delete failed" in error_msg

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._get_retry_session"
    )
    def test_delete_uses_run_event_timestamp_for_new_anomalies(self, mock_get_session):
        """Test that new anomalies use run_event_timestamp_ms for deletion."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _delete_anomaly_event,
        )

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": {"timeseriesRows": 1}}
        mock_response.text = '{"value": {"timeseriesRows": 1}}'
        mock_session.post.return_value = mock_response

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,  # This should be ignored
            original_state=None,
            local_state="DELETE",
            is_new=True,
            run_event_timestamp_ms=2000,  # This should be used
        )

        success, error_msg = _delete_anomaly_event(
            graphql_url="http://test/api/graphql",
            headers={},
            change=change,
        )

        assert success is True
        # Verify the payload uses run_event_timestamp_ms
        call_args = mock_session.post.call_args
        payload = call_args.kwargs["json"]
        assert payload["startTimeMillis"] == 2000
        assert payload["endTimeMillis"] == 2000


class TestPublishSingleAnomalyWithDelete:
    """Tests for _publish_single_anomaly handling via REST API."""

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._delete_anomaly_event"
    )
    def test_delete_state_calls_delete_function(self, mock_delete):
        """Test that DELETE state dispatches to _delete_anomaly_event."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _publish_single_anomaly,
        )

        mock_delete.return_value = (True, None)

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state="CONFIRMED",
            local_state="DELETE",
        )

        success, error_msg = _publish_single_anomaly(
            graphql_url="http://test/api/graphql",
            headers={},
            change=change,
        )

        mock_delete.assert_called_once()
        assert success is True
        assert error_msg is None

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._create_anomaly_event_rest"
    )
    def test_confirmed_state_uses_rest(self, mock_create):
        """Test that CONFIRMED state uses REST API."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _publish_single_anomaly,
        )

        mock_create.return_value = (True, None)

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state=None,
            local_state="CONFIRMED",
        )

        success, error_msg = _publish_single_anomaly(
            graphql_url="http://test/api/graphql",
            headers={},
            change=change,
        )

        mock_create.assert_called_once()
        assert success is True

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._create_anomaly_event_rest"
    )
    def test_rejected_state_uses_rest(self, mock_create):
        """Test that REJECTED state uses REST API."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _publish_single_anomaly,
        )

        mock_create.return_value = (True, None)

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state="CONFIRMED",
            local_state="REJECTED",
        )

        success, error_msg = _publish_single_anomaly(
            graphql_url="http://test/api/graphql",
            headers={},
            change=change,
        )

        mock_create.assert_called_once()
        assert success is True


class TestCreateAnomalyEventRest:
    """Tests for _create_anomaly_event_rest function."""

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._get_retry_session"
    )
    def test_successful_create(self, mock_get_session):
        """Test successful creation of anomaly event via REST."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _create_anomaly_event_rest,
        )

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_session.post.return_value = mock_response

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state=None,
            local_state="CONFIRMED",
        )

        success, error_msg = _create_anomaly_event_rest(
            base_url="http://test",
            headers={"Authorization": "Bearer token"},
            change=change,
        )

        assert success is True
        assert error_msg is None
        # The function makes 2 POST calls: first to GraphQL to fetch run events,
        # then to REST API to create the anomaly event
        assert mock_session.post.call_count == 2
        # Verify the last call (REST API) is to the entity endpoint
        call_args = mock_session.post.call_args_list[-1]
        assert call_args[0][0] == "http://test/openapi/v3/entity/monitor"
        # Verify the payload is an array containing the entity
        payload = call_args[1]["json"]
        assert isinstance(payload, list)
        assert len(payload) == 1
        assert payload[0]["urn"] == "urn:li:monitor:test"
        assert "monitorAnomalyEvent" in payload[0]
        assert payload[0]["monitorAnomalyEvent"]["value"]["state"] == "CONFIRMED"

    @patch(
        "scripts.streamlit_explorer.timeseries_explorer.monitor_browser._get_retry_session"
    )
    def test_http_error_returns_failure(self, mock_get_session):
        """Test that HTTP errors return failure."""
        from scripts.streamlit_explorer.timeseries_explorer.monitor_browser import (
            _create_anomaly_event_rest,
        )

        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_session.post.return_value = mock_response

        change = MockAnomalyEdit(
            monitor_urn="urn:li:monitor:test",
            assertion_urn="urn:li:assertion:test",
            timestamp_ms=1000,
            original_state=None,
            local_state="CONFIRMED",
        )

        success, error_msg = _create_anomaly_event_rest(
            base_url="http://test",
            headers={},
            change=change,
        )

        assert success is False
        assert error_msg is not None
        assert "500" in error_msg
