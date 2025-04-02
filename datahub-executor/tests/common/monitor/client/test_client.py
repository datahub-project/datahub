from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AnomalySourceClass,
    AnomalySourcePropertiesClass,
    AssertionEvaluationContextClass,
    AssertionInfoClass,
    AssertionMetricClass,
    FreshnessFieldSpecClass,
    MonitorAnomalyEventClass,
    MonitorErrorClass,
    MonitorStateClass,
    MonitorTypeClass,
    TimeStampClass,
)

from datahub_executor.common.metric.types import (
    Metric,
)
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.types import Anomaly, CronSchedule


@pytest.fixture
def mock_graph() -> MagicMock:
    """Create a mock DataHubGraph instance."""
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def monitor_client(mock_graph: MagicMock) -> MonitorClient:
    """Create a MonitorClient instance with mock dependencies."""
    return MonitorClient(graph=mock_graph)


@pytest.fixture
def test_urn() -> str:
    """Return a test entity URN."""
    return "urn:li:assertion:test-assertion"


@pytest.fixture
def test_monitor_urn() -> str:
    """Return a test monitor URN."""
    return "urn:li:dataHubAssetMonitor:test-monitor"


@pytest.fixture
def test_assertion_info() -> AssertionInfoClass:
    """Return a test AssertionInfo instance."""
    return AssertionInfoClass(type="DATASET")


@pytest.fixture
def test_evaluation_context() -> AssertionEvaluationContextClass:
    """Return a test evaluation context."""
    return AssertionEvaluationContextClass(
        embeddedAssertions=None, inferenceDetails=None, stdDev=None
    )


@pytest.fixture
def test_schedule() -> CronSchedule:
    """Return a test schedule."""
    return CronSchedule(cron="0 * * * *", timezone="UTC")


@pytest.fixture
def sample_anomalies() -> List[Anomaly]:
    """Create sample anomaly data for testing."""
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    return [
        Anomaly(
            timestamp_ms=now - 3600000,
            metric=Metric(value=100.0, timestamp_ms=now - 3600000),
        ),  # 1 hour ago
        Anomaly(
            timestamp_ms=now - 7200000,
            metric=Metric(value=200.0, timestamp_ms=now - 7200000),
        ),  # 2 hours ago
        Anomaly(
            timestamp_ms=now - 10800000,
            metric=Metric(value=300.0, timestamp_ms=now - 10800000),
        ),  # 3 hours ago
    ]


class TestMonitorClient:
    def test_update_assertion_info(
        self,
        monitor_client: MonitorClient,
        test_urn: str,
        test_assertion_info: AssertionInfoClass,
        mock_graph: MagicMock,
    ) -> None:
        """Test updating assertion info."""
        # Call method
        monitor_client.update_assertion_info(test_urn, test_assertion_info)

        # Verify graph.emit_mcps was called with the correct MetadataChangeProposalWrapper
        mock_graph.emit_mcps.assert_called_once()
        args = mock_graph.emit_mcps.call_args[0][0]
        assert len(args) == 1
        assert isinstance(args[0], MetadataChangeProposalWrapper)
        assert args[0].entityUrn == test_urn
        assert args[0].aspect == test_assertion_info

    @patch("datahub_executor.common.monitor.client.client.MonitorPatchBuilder")
    def test_patch_volume_monitor_evaluation_context(
        self,
        mock_patch_builder_class: MagicMock,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        test_urn: str,
        test_evaluation_context: AssertionEvaluationContextClass,
        mock_graph: MagicMock,
    ) -> None:
        """Test patching volume monitor evaluation context."""
        # Create a mock volume evaluation spec
        mock_volume_spec = MagicMock()
        mock_volume_spec.schedule.cron = "0 0 * * *"
        mock_volume_spec.schedule.timezone = "UTC"
        mock_volume_spec.parameters.dataset_volume_parameters.source_type.value = (
            "QUERY"
        )

        # Setup mock patch builder
        mock_patch_builder = MagicMock()
        mock_patch_builder_class.return_value = mock_patch_builder
        mock_patch_builder.build.return_value = ["mock_mcp"]

        # Call method
        monitor_client.patch_volume_monitor_evaluation_context(
            test_monitor_urn,
            test_urn,
            test_evaluation_context,
            mock_volume_spec,
        )

        # Verify the patch builder was created with the correct URN
        mock_patch_builder_class.assert_called_once_with(urn=test_monitor_urn)

        # Verify set_type was called with ASSERTION
        mock_patch_builder.set_type.assert_called_once_with(MonitorTypeClass.ASSERTION)

        # Verify set_assertion_monitor_assertions was called
        mock_patch_builder.set_assertion_monitor_assertions.assert_called_once()

        # Verify assertions list was passed (we can't easily check exact contents with mocks)
        assertions = mock_patch_builder.set_assertion_monitor_assertions.call_args[1][
            "assertions"
        ]
        assert isinstance(assertions, list)
        assert len(assertions) == 1

        # Verify graph.emit_mcps was called with the MCPs returned by the patch builder
        mock_graph.emit_mcps.assert_called_once_with(["mock_mcp"])

    def test_patch_volume_monitor_evaluation_context_missing_params(
        self,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        test_urn: str,
        test_evaluation_context: AssertionEvaluationContextClass,
    ) -> None:
        """Test that an exception is raised when volume evaluation spec is missing required params."""
        # Create a mock with missing parameters
        mock_incomplete_spec = MagicMock()
        mock_incomplete_spec.schedule = MagicMock()
        mock_incomplete_spec.parameters.dataset_volume_parameters = None

        # Verify exception is raised
        with pytest.raises(Exception) as excinfo:
            monitor_client.patch_volume_monitor_evaluation_context(
                test_monitor_urn,
                test_urn,
                test_evaluation_context,
                mock_incomplete_spec,
            )

        assert "Failed to update volume assertion monitor evaluation context" in str(
            excinfo.value
        )

    @patch("datahub_executor.common.monitor.client.client.MonitorPatchBuilder")
    def test_patch_freshness_monitor_evaluation_spec(
        self,
        mock_patch_builder_class: MagicMock,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        test_urn: str,
        test_evaluation_context: AssertionEvaluationContextClass,
        test_schedule: CronSchedule,
        mock_graph: MagicMock,
    ) -> None:
        """Test patching freshness monitor evaluation context."""
        # Create a mock freshness evaluation spec
        mock_freshness_spec = MagicMock()
        mock_freshness_spec.schedule.cron = "0 0 * * *"
        mock_freshness_spec.schedule.timezone = "UTC"
        mock_freshness_spec.parameters.dataset_freshness_parameters.source_type.value = "FIELD_VALUE"
        mock_freshness_spec.parameters.dataset_freshness_parameters.field.path = (
            "updated_at"
        )
        mock_freshness_spec.parameters.dataset_freshness_parameters.field.type = (
            "TIMESTAMP"
        )
        mock_freshness_spec.parameters.dataset_freshness_parameters.field.native_type = "TIMESTAMP"
        mock_freshness_spec.parameters.dataset_freshness_parameters.field.kind.value = (
            "LAST_MODIFIED"
        )
        mock_freshness_spec.parameters.dataset_freshness_parameters.audit_log = None

        # Setup mock patch builder
        mock_patch_builder = MagicMock()
        mock_patch_builder_class.return_value = mock_patch_builder
        mock_patch_builder.build.return_value = ["mock_mcp"]

        # Call method
        monitor_client.patch_freshness_monitor_evaluation_spec(
            test_monitor_urn,
            test_urn,
            test_evaluation_context,
            test_schedule,
            mock_freshness_spec,
        )

        # Verify the patch builder was created with the correct URN
        mock_patch_builder_class.assert_called_once_with(urn=test_monitor_urn)

        # Verify set_type was called with ASSERTION
        mock_patch_builder.set_type.assert_called_once_with(MonitorTypeClass.ASSERTION)

        # Verify set_assertion_monitor_assertions was called
        mock_patch_builder.set_assertion_monitor_assertions.assert_called_once()

        # Verify assertions list was passed
        assertions = mock_patch_builder.set_assertion_monitor_assertions.call_args[1][
            "assertions"
        ]
        assert isinstance(assertions, list)
        assert len(assertions) == 1

        # Verify graph.emit_mcps was called with the MCPs returned by the patch builder
        mock_graph.emit_mcps.assert_called_once_with(["mock_mcp"])

    def test_patch_freshness_monitor_evaluation_spec_missing_params(
        self,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        test_urn: str,
        test_evaluation_context: AssertionEvaluationContextClass,
        test_schedule: CronSchedule,
    ) -> None:
        """Test that an exception is raised when freshness evaluation spec is missing required params."""
        # Create a mock with missing parameters
        mock_incomplete_spec = MagicMock()
        mock_incomplete_spec.schedule = MagicMock()
        mock_incomplete_spec.parameters.dataset_freshness_parameters = None

        # Verify exception is raised
        with pytest.raises(Exception) as excinfo:
            monitor_client.patch_freshness_monitor_evaluation_spec(
                test_monitor_urn,
                test_urn,
                test_evaluation_context,
                test_schedule,
                mock_incomplete_spec,
            )

        assert "Failed to update freshness assertion monitor evaluation context" in str(
            excinfo.value
        )

    @patch("datahub_executor.common.monitor.client.client.MonitorPatchBuilder")
    def test_patch_field_metric_monitor_evaluation_context(
        self,
        mock_patch_builder_class: MagicMock,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        test_urn: str,
        test_evaluation_context: AssertionEvaluationContextClass,
        mock_graph: MagicMock,
    ) -> None:
        """Test patching field metric monitor evaluation context."""
        # Create a mock field metric evaluation spec
        mock_field_metric_spec = MagicMock()
        mock_field_metric_spec.schedule.cron = "0 0 * * *"
        mock_field_metric_spec.schedule.timezone = "UTC"
        mock_field_metric_spec.parameters.dataset_field_parameters.source_type.value = (
            "ALL_ROWS_QUERY"
        )
        mock_field_metric_spec.parameters.dataset_field_parameters.changed_rows_field.path = "updated_at"
        mock_field_metric_spec.parameters.dataset_field_parameters.changed_rows_field.type = "TIMESTAMP"
        mock_field_metric_spec.parameters.dataset_field_parameters.changed_rows_field.native_type = "TIMESTAMP"
        mock_field_metric_spec.parameters.dataset_field_parameters.changed_rows_field.kind.value = "LAST_MODIFIED"

        # Setup mock patch builder
        mock_patch_builder = MagicMock()
        mock_patch_builder_class.return_value = mock_patch_builder
        mock_patch_builder.build.return_value = ["mock_mcp"]

        # Call method
        monitor_client.patch_field_metric_monitor_evaluation_context(
            test_monitor_urn,
            test_urn,
            test_evaluation_context,
            mock_field_metric_spec,
        )

        # Verify the patch builder was created with the correct URN
        mock_patch_builder_class.assert_called_once_with(urn=test_monitor_urn)

        # Verify set_type was called with ASSERTION
        mock_patch_builder.set_type.assert_called_once_with(MonitorTypeClass.ASSERTION)

        # Verify set_assertion_monitor_assertions was called
        mock_patch_builder.set_assertion_monitor_assertions.assert_called_once()

        # Verify assertions list was passed
        assertions = mock_patch_builder.set_assertion_monitor_assertions.call_args[1][
            "assertions"
        ]
        assert isinstance(assertions, list)
        assert len(assertions) == 1

        # Verify graph.emit_mcps was called with the MCPs returned by the patch builder
        mock_graph.emit_mcps.assert_called_once_with(["mock_mcp"])

    def test_patch_field_metric_monitor_evaluation_context_missing_params(
        self,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        test_urn: str,
        test_evaluation_context: AssertionEvaluationContextClass,
    ) -> None:
        """Test that an exception is raised when field metric evaluation spec is missing required params."""
        # Create a mock with missing parameters
        mock_incomplete_spec = MagicMock()
        mock_incomplete_spec.schedule = MagicMock()
        mock_incomplete_spec.parameters.dataset_field_parameters = None

        # Verify exception is raised
        with pytest.raises(Exception) as excinfo:
            monitor_client.patch_field_metric_monitor_evaluation_context(
                test_monitor_urn,
                test_urn,
                test_evaluation_context,
                mock_incomplete_spec,
            )

        assert (
            "Failed to update field metric assertion monitor evaluation context"
            in str(excinfo.value)
        )

    @patch("datahub_executor.common.monitor.client.client.MonitorPatchBuilder")
    def test_patch_monitor_state(
        self,
        mock_patch_builder_class: MagicMock,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        mock_graph: MagicMock,
    ) -> None:
        """Test patching monitor state."""
        # Setup mock patch builder
        mock_patch_builder = MagicMock()
        mock_patch_builder_class.return_value = mock_patch_builder
        mock_patch_builder.build.return_value = ["mock_mcp"]

        # Create a MonitorStateClass instance for the test instead of using an enum
        new_state = MonitorStateClass.EVALUATION
        error = MonitorErrorClass(type="UNKNOWN", message="Test error")

        # Call method
        monitor_client.patch_monitor_state(
            test_monitor_urn,
            new_state,
            error,
        )

        # Verify the patch builder was created with the correct URN
        mock_patch_builder_class.assert_called_once_with(urn=test_monitor_urn)

        # Verify set_state was called with the new state
        mock_patch_builder.set_state.assert_called_once_with(new_state)

        # Verify set_error was called with the error
        mock_patch_builder.set_error.assert_called_once_with(error)

        # Verify graph.emit_mcps was called with the MCPs returned by the patch builder
        mock_graph.emit_mcps.assert_called_once_with(["mock_mcp"])

    @patch("datahub_executor.common.monitor.client.client.MonitorPatchBuilder")
    def test_helper_methods(
        self,
        mock_patch_builder_class: MagicMock,
        monitor_client: MonitorClient,
    ) -> None:
        """Test the helper methods directly to ensure they work as expected."""
        # Setup mock objects instead of trying to create real objects
        mock_freshness_spec = MagicMock()
        mock_freshness_spec.parameters.dataset_freshness_parameters.field.path = (
            "updated_at"
        )
        mock_freshness_spec.parameters.dataset_freshness_parameters.field.type = (
            "TIMESTAMP"
        )
        mock_freshness_spec.parameters.dataset_freshness_parameters.field.native_type = "TIMESTAMP"
        mock_freshness_spec.parameters.dataset_freshness_parameters.field.kind.value = (
            "LAST_MODIFIED"
        )

        mock_field_metric_spec = MagicMock()
        mock_field_metric_spec.parameters.dataset_field_parameters.changed_rows_field.path = "updated_at"
        mock_field_metric_spec.parameters.dataset_field_parameters.changed_rows_field.type = "TIMESTAMP"
        mock_field_metric_spec.parameters.dataset_field_parameters.changed_rows_field.native_type = "TIMESTAMP"
        mock_field_metric_spec.parameters.dataset_field_parameters.changed_rows_field.kind.value = "LAST_MODIFIED"

        # Mock for _create_base_monitor_patch_builder method
        mock_builder = MagicMock()
        mock_patch_builder_class.return_value = mock_builder

        # Test _build_freshness_field_spec with mocked input
        field_spec = monitor_client._build_freshness_field_spec(mock_freshness_spec)
        assert field_spec is not None
        assert isinstance(field_spec, FreshnessFieldSpecClass)

        # Test _build_field_metric_changed_rows_field_spec with mocked input
        field_spec = monitor_client._build_field_metric_changed_rows_field_spec(
            mock_field_metric_spec
        )
        assert field_spec is not None
        assert isinstance(field_spec, FreshnessFieldSpecClass)

        # Test _create_base_monitor_patch_builder
        builder = monitor_client._create_base_monitor_patch_builder("test_urn")
        assert builder == mock_builder
        mock_patch_builder_class.assert_called_with(urn="test_urn")
        mock_builder.set_type.assert_called_with(MonitorTypeClass.ASSERTION)

    def test_fetch_monitor_anomalies(
        self,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        mock_graph: MagicMock,
        sample_anomalies: List[Anomaly],
    ) -> None:
        """Test fetching metric values with lookback."""
        # Setup mock response
        mock_anomaly_events = [
            self._create_monitor_anomaly_event(anomaly) for anomaly in sample_anomalies
        ]
        mock_graph.get_timeseries_values.return_value = mock_anomaly_events

        # Test
        with patch("datetime.datetime", wraps=datetime) as mock_datetime:
            # Mock now() to return a fixed time
            fixed_now = datetime(2022, 1, 1, tzinfo=timezone.utc)
            mock_datetime.now.return_value = fixed_now

            result = monitor_client.fetch_monitor_anomalies(
                test_monitor_urn,
                lookback=timedelta(days=7),
                limit=200,
            )

        # Verify the result
        assert len(result) == len(sample_anomalies)
        for i, anomaly in enumerate(result):
            assert anomaly.timestamp_ms == sample_anomalies[i].timestamp_ms
            assert anomaly.metric.value == sample_anomalies[i].metric.value  # type: ignore
            assert (
                anomaly.metric.timestamp_ms == sample_anomalies[i].metric.timestamp_ms  # type: ignore
            )  # type: ignore

        # Verify that get_timeseries_values was called with correct params
        mock_graph.get_timeseries_values.assert_called_once()
        call_args = mock_graph.get_timeseries_values.call_args[1]
        assert call_args["entity_urn"] == test_monitor_urn
        assert call_args["aspect_type"] == MonitorAnomalyEventClass
        assert call_args["limit"] == 200

    def test_fetch_monitor_anomalies_by_time(
        self,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        mock_graph: MagicMock,
        sample_anomalies: List[Anomaly],
    ) -> None:
        """Test fetching anomalies by specific time range."""
        # Setup mock response
        mock_anomaly_events = [
            self._create_monitor_anomaly_event(anomaly) for anomaly in sample_anomalies
        ]
        mock_graph.get_timeseries_values.return_value = mock_anomaly_events

        # Test with specific time range
        start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 7, tzinfo=timezone.utc)
        result = monitor_client.fetch_monitor_anomalies_by_time(
            test_monitor_urn, start_time=start_time, end_time=end_time, limit=100
        )

        # Verify the result
        assert len(result) == len(sample_anomalies)
        for i, anomaly in enumerate(result):
            assert anomaly.timestamp_ms == sample_anomalies[i].timestamp_ms
            assert anomaly.metric.value == sample_anomalies[i].metric.value  # type: ignore
            assert (
                anomaly.metric.timestamp_ms == sample_anomalies[i].metric.timestamp_ms  # type: ignore
            )  # type: ignore

        # Verify that get_timeseries_values was called with correct params
        mock_graph.get_timeseries_values.assert_called_once()
        call_args = mock_graph.get_timeseries_values.call_args[1]
        assert call_args["entity_urn"] == test_monitor_urn
        assert call_args["aspect_type"] == MonitorAnomalyEventClass
        assert call_args["limit"] == 100

        # Verify time filter
        time_filter = call_args["filter"]["or"][0]["and"]
        assert time_filter[0]["field"] == "timestampMillis"
        assert time_filter[0]["condition"] == "GREATER_THAN_OR_EQUAL_TO"
        assert time_filter[0]["value"] == str(int(start_time.timestamp() * 1000))
        assert time_filter[1]["field"] == "timestampMillis"
        assert time_filter[1]["condition"] == "LESS_THAN_OR_EQUAL_TO"
        assert time_filter[1]["value"] == str(int(end_time.timestamp() * 1000))

    def _create_monitor_anomaly_event(
        self, anomaly: Anomaly
    ) -> MonitorAnomalyEventClass:
        return MonitorAnomalyEventClass(
            timestampMillis=anomaly.timestamp_ms,
            source=AnomalySourceClass(
                type="INFERRED_ASSERTION_FAILURE",
                properties=AnomalySourcePropertiesClass(
                    assertionMetric=AssertionMetricClass(
                        timestampMs=anomaly.metric.timestamp_ms,  # type: ignore
                        value=anomaly.metric.value,  # type: ignore
                    )
                ),
            ),
            state="CONFIRMED",
            # TODO: Determine whether we really need these audit fields.
            created=TimeStampClass(time=0),
            lastUpdated=TimeStampClass(time=0),
        )
