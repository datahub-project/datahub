import json
from unittest.mock import MagicMock, patch

import pytest
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    AssertionEvaluationSpecClass,
    CronScheduleClass,
    MonitorInfoClass,
    MonitorStateClass,
    MonitorStatusClass,
    MonitorTypeClass,
    SystemMetadataClass,
)

from datahub_executor.common.monitor.client.patch_builder import MonitorPatchBuilder


@pytest.fixture
def monitor_urn() -> str:
    """Return a test monitor URN."""
    return "urn:li:dataHubAssetMonitor:test-monitor"


@pytest.fixture
def system_metadata() -> SystemMetadataClass:
    """Return a test system metadata instance."""
    return SystemMetadataClass(
        lastObserved=1234567890,
        runId="test-run-id",
    )


class TestMonitorPatchBuilder:
    """Tests for the MonitorPatchBuilder class."""

    def test_init(self, monitor_urn: str, system_metadata: SystemMetadataClass) -> None:
        """Test initialization of the MonitorPatchBuilder class."""
        # Test with just URN
        builder = MonitorPatchBuilder(urn=monitor_urn)
        assert builder.urn == monitor_urn
        assert builder.system_metadata is None
        assert builder.audit_header is None

        # Test with system metadata
        builder = MonitorPatchBuilder(urn=monitor_urn, system_metadata=system_metadata)
        assert builder.urn == monitor_urn
        assert builder.system_metadata == system_metadata
        assert builder.audit_header is None

        # Test with audit header
        audit_header = MagicMock()
        builder = MonitorPatchBuilder(
            urn=monitor_urn, system_metadata=system_metadata, audit_header=audit_header
        )
        assert builder.urn == monitor_urn
        assert builder.system_metadata == system_metadata
        assert builder.audit_header == audit_header

    def test_set_type(self, monitor_urn: str) -> None:
        """Test setting the monitor type."""
        builder = MonitorPatchBuilder(urn=monitor_urn)

        # Test with enum value
        result = builder.set_type(MonitorTypeClass.ASSERTION)
        assert result == builder  # Should return self for chaining

        # Verify patch was added correctly
        patches = builder.build()
        assert len(patches) == 1
        mcp = patches[0]

        # Verify basic properties of the MCP
        assert mcp.entityUrn == monitor_urn
        assert mcp.aspectName == MonitorInfoClass.ASPECT_NAME
        assert mcp.changeType == "PATCH"

        # Check that patch contains expected operation
        # The patch is stored as JSON in the aspect's value field
        patch_json = mcp.aspect.value  # type: ignore
        patch_data = json.loads(patch_json.decode("utf-8"))
        assert len(patch_data) == 1
        assert patch_data[0]["op"] == "add"
        assert patch_data[0]["path"] == "/type"
        assert patch_data[0]["value"] == "ASSERTION"

        # Test with string value
        builder = MonitorPatchBuilder(urn=monitor_urn)
        builder.set_type("ASSERTION")
        patches = builder.build()

        patch_json = patches[0].aspect.value  # type: ignore
        patch_data = json.loads(patch_json.decode("utf-8"))
        assert patch_data[0]["op"] == "add"
        assert patch_data[0]["path"] == "/type"
        assert patch_data[0]["value"] == "ASSERTION"

    def test_set_assertion_monitor_assertions(self, monitor_urn: str) -> None:
        """Test setting assertion monitor assertions."""
        builder = MonitorPatchBuilder(urn=monitor_urn)

        # Create test assertions
        assertions = [
            AssertionEvaluationSpecClass(
                assertion="urn:li:assertion:test-assertion-1",
                schedule=CronScheduleClass(
                    cron="* * * * *", timezone="America/Los Angeles"
                ),
                parameters=None,
                context=None,
            ),
            AssertionEvaluationSpecClass(
                assertion="urn:li:assertion:test-assertion-2",
                schedule=CronScheduleClass(
                    cron="* * * * *", timezone="America/Los Angeles"
                ),
                parameters=None,
                context=None,
            ),
        ]

        # Set assertions
        result = builder.set_assertion_monitor_assertions(assertions)
        assert result == builder  # Should return self for chaining

        # Verify patch was added correctly
        patches = builder.build()
        assert len(patches) == 1
        mcp = patches[0]

        # Verify basic properties
        assert mcp.entityUrn == monitor_urn
        assert mcp.aspectName == MonitorInfoClass.ASPECT_NAME

        # Extract and check patch data
        patch_json = mcp.aspect.value  # type: ignore
        patch_data = json.loads(patch_json.decode("utf-8"))
        assert len(patch_data) == 1
        assert patch_data[0]["op"] == "add"
        assert patch_data[0]["path"] == "/assertionMonitor/assertions"

        # Verify assertions in the value
        value = patch_data[0]["value"]
        assert len(value) == 2
        assert value[0]["assertion"] == "urn:li:assertion:test-assertion-1"
        assert value[0]["schedule"]["cron"] == "* * * * *"
        assert value[0]["schedule"]["timezone"] == "America/Los Angeles"
        assert value[1]["assertion"] == "urn:li:assertion:test-assertion-2"

    def test_set_status(self, monitor_urn: str) -> None:
        """Test setting the monitor status."""
        builder = MonitorPatchBuilder(urn=monitor_urn)

        # Create test status
        status = MonitorStatusClass(mode="ACTIVE")

        # Set status
        result = builder.set_status(status)
        assert result == builder  # Should return self for chaining

        # Verify patch was added correctly
        patches = builder.build()
        assert len(patches) == 1
        mcp = patches[0]

        # Verify basic properties
        assert mcp.entityUrn == monitor_urn
        assert mcp.aspectName == MonitorInfoClass.ASPECT_NAME

        # Extract and check patch data
        patch_json = mcp.aspect.value  # type: ignore
        patch_data = json.loads(patch_json.decode("utf-8"))
        assert len(patch_data) == 1
        assert patch_data[0]["op"] == "add"
        assert patch_data[0]["path"] == "/status"
        assert patch_data[0]["value"]["mode"] == "ACTIVE"

    def test_set_state(self, monitor_urn: str) -> None:
        """Test setting the monitor state."""
        builder = MonitorPatchBuilder(urn=monitor_urn)

        # Test with enum value
        result = builder.set_state(MonitorStateClass.EVALUATION)
        assert result == builder  # Should return self for chaining

        # Verify patch was added correctly
        patches = builder.build()
        assert len(patches) == 1
        mcp = patches[0]

        # Verify basic properties
        assert mcp.entityUrn == monitor_urn
        assert mcp.aspectName == MonitorInfoClass.ASPECT_NAME

        # Extract and check patch data
        patch_json = mcp.aspect.value  # type: ignore
        patch_data = json.loads(patch_json.decode("utf-8"))
        assert len(patch_data) == 1
        assert patch_data[0]["op"] == "add"
        assert patch_data[0]["path"] == "/status/state"
        assert patch_data[0]["value"] == "EVALUATION"

        # Test with string value
        builder = MonitorPatchBuilder(urn=monitor_urn)
        builder.set_state("EVALUATION")
        patches = builder.build()

        patch_json = patches[0].aspect.value  # type: ignore
        patch_data = json.loads(patch_json.decode("utf-8"))
        assert patch_data[0]["op"] == "add"
        assert patch_data[0]["path"] == "/status/state"
        assert patch_data[0]["value"] == "EVALUATION"

    def test_set_error(self, monitor_urn: str) -> None:
        """Test setting the monitor error."""
        builder = MonitorPatchBuilder(urn=monitor_urn)

        # Use a string value instead of class instance to avoid serialization issues
        result = builder.set_error("Test error message")
        assert result == builder  # Should return self for chaining

        # Verify patch was added correctly
        patches = builder.build()
        assert len(patches) == 1
        mcp = patches[0]

        # Verify basic properties
        assert mcp.entityUrn == monitor_urn
        assert mcp.aspectName == MonitorInfoClass.ASPECT_NAME

        # Extract and check patch data
        patch_json = mcp.aspect.value  # type: ignore
        patch_data = json.loads(patch_json.decode("utf-8"))
        assert len(patch_data) == 1
        assert patch_data[0]["op"] == "add"
        assert patch_data[0]["path"] == "/status/error"
        assert patch_data[0]["value"] == "Test error message"

    def test_chaining_methods(self, monitor_urn: str) -> None:
        """Test chaining multiple methods together."""
        builder = MonitorPatchBuilder(urn=monitor_urn)

        # Chain multiple methods using string values to avoid serialization issues
        assertions = [
            AssertionEvaluationSpecClass(
                assertion="urn:li:assertion:test-assertion-1",
                schedule=CronScheduleClass(
                    cron="* * * * *", timezone="America/Los Angeles"
                ),
                parameters=None,
                context=None,
            )
        ]

        builder.set_type(MonitorTypeClass.ASSERTION).set_assertion_monitor_assertions(
            assertions
        ).set_state("EVALUATION").set_error("Test error message")

        # Verify all patches were added
        patches = builder.build()
        assert len(patches) == 1
        mcp = patches[0]

        # Extract patch operations
        patch_json = mcp.aspect.value  # type: ignore
        patch_data = json.loads(patch_json.decode("utf-8"))
        assert len(patch_data) == 4

        # Verify each operation path
        paths = [op["path"] for op in patch_data]
        assert "/type" in paths
        assert "/assertionMonitor/assertions" in paths
        assert "/status/state" in paths
        assert "/status/error" in paths

    @patch.object(MetadataPatchProposal, "_add_patch")
    def test_underlying_add_patch_calls(
        self, mock_add_patch: MagicMock, monitor_urn: str
    ) -> None:
        """Test that the methods correctly call the underlying _add_patch method."""
        builder = MonitorPatchBuilder(urn=monitor_urn)

        # Test set_type
        builder.set_type(MonitorTypeClass.ASSERTION)
        mock_add_patch.assert_called_with(
            MonitorInfoClass.ASPECT_NAME,
            "add",
            path=("type",),
            value=MonitorTypeClass.ASSERTION,
        )

        # Test set_assertion_monitor_assertions
        assertions = [
            AssertionEvaluationSpecClass(
                assertion="urn:li:assertion:test-assertion",
                schedule=CronScheduleClass(
                    cron="* * * * *", timezone="America/Los Angeles"
                ),
                parameters=None,
                context=None,
            )
        ]
        builder.set_assertion_monitor_assertions(assertions)
        mock_add_patch.assert_called_with(
            MonitorInfoClass.ASPECT_NAME,
            "add",
            path=("assertionMonitor", "assertions"),
            value=assertions,
        )

        # Test set_status
        status = MonitorStatusClass(mode="ACTIVE")
        builder.set_status(status)
        mock_add_patch.assert_called_with(
            MonitorInfoClass.ASPECT_NAME, "add", path=("status",), value=status
        )

        # Test set_state
        state = MonitorStateClass.EVALUATION
        builder.set_state(state)
        mock_add_patch.assert_called_with(
            MonitorInfoClass.ASPECT_NAME, "add", path=("status", "state"), value=state
        )

        # Test set_error
        error = "Test error"
        builder.set_error(error)
        mock_add_patch.assert_called_with(
            MonitorInfoClass.ASPECT_NAME, "add", path=("status", "error"), value=error
        )
