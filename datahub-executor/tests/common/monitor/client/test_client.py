from unittest.mock import MagicMock, patch

import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInfoClass,
    FreshnessFieldSpecClass,
    MonitorErrorClass,
    MonitorStateClass,
    MonitorTypeClass,
)

from datahub_executor.common.monitor.client.client import MonitorClient


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
    def test_patch_freshness_monitor_evaluation_context(
        self,
        mock_patch_builder_class: MagicMock,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        test_urn: str,
        test_evaluation_context: AssertionEvaluationContextClass,
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
        monitor_client.patch_freshness_monitor_evaluation_context(
            test_monitor_urn,
            test_urn,
            test_evaluation_context,
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

    def test_patch_freshness_monitor_evaluation_context_missing_params(
        self,
        monitor_client: MonitorClient,
        test_monitor_urn: str,
        test_urn: str,
        test_evaluation_context: AssertionEvaluationContextClass,
    ) -> None:
        """Test that an exception is raised when freshness evaluation spec is missing required params."""
        # Create a mock with missing parameters
        mock_incomplete_spec = MagicMock()
        mock_incomplete_spec.schedule = MagicMock()
        mock_incomplete_spec.parameters.dataset_freshness_parameters = None

        # Verify exception is raised
        with pytest.raises(Exception) as excinfo:
            monitor_client.patch_freshness_monitor_evaluation_context(
                test_monitor_urn,
                test_urn,
                test_evaluation_context,
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
