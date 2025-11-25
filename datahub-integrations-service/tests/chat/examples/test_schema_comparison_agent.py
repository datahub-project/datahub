"""Unit tests for schema comparison agent."""

from unittest.mock import Mock, patch

from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.chat.examples.schema_comparison_agent import (
    create_schema_comparison_agent,
    report_schema_differences,
    respond_with_schema_report,
)


class TestReportSchemaDifferences:
    """Test report_schema_differences function."""

    def test_report_with_added_fields(self) -> None:
        """Test reporting schema differences with added fields."""
        result = report_schema_differences(
            dataset_a_urn="urn:li:dataset:a",
            dataset_b_urn="urn:li:dataset:b",
            added_fields=["new_field1", "new_field2"],
            removed_fields=[],
            modified_fields=[],
            breaking_changes=False,
        )

        assert result["comparison"]["dataset_a"] == "urn:li:dataset:a"
        assert result["comparison"]["dataset_b"] == "urn:li:dataset:b"
        assert result["changes"]["added_fields"] == ["new_field1", "new_field2"]
        assert result["changes"]["removed_fields"] == []
        assert result["changes"]["modified_fields"] == []
        assert result["impact"]["breaking_changes"] is False
        assert result["impact"]["severity"] == "LOW"
        assert len(result["recommendations"]) == 0

    def test_report_with_removed_fields(self) -> None:
        """Test reporting with removed fields (breaking change)."""
        result = report_schema_differences(
            dataset_a_urn="urn:li:dataset:a",
            dataset_b_urn="urn:li:dataset:b",
            added_fields=[],
            removed_fields=["old_field1", "old_field2"],
            modified_fields=[],
            breaking_changes=False,
        )

        assert result["changes"]["removed_fields"] == ["old_field1", "old_field2"]
        assert any("Review downstream" in rec for rec in result["recommendations"])

    def test_report_with_modified_fields(self) -> None:
        """Test reporting with modified fields."""
        modified = [
            {
                "field_name": "field1",
                "old_type": "STRING",
                "new_type": "INT",
            }
        ]

        result = report_schema_differences(
            dataset_a_urn="urn:li:dataset:a",
            dataset_b_urn="urn:li:dataset:b",
            added_fields=[],
            removed_fields=[],
            modified_fields=modified,
            breaking_changes=False,
        )

        assert result["changes"]["modified_fields"] == modified
        assert result["impact"]["severity"] == "MEDIUM"
        assert any("data contract" in rec.lower() for rec in result["recommendations"])

    def test_report_with_breaking_changes(self) -> None:
        """Test reporting with breaking changes."""
        result = report_schema_differences(
            dataset_a_urn="urn:li:dataset:a",
            dataset_b_urn="urn:li:dataset:b",
            added_fields=[],
            removed_fields=["field1"],
            modified_fields=[
                {"field_name": "field2", "old_type": "INT", "new_type": "STRING"}
            ],
            breaking_changes=True,
        )

        assert result["impact"]["breaking_changes"] is True
        assert result["impact"]["severity"] == "HIGH"
        assert any("staged rollout" in rec for rec in result["recommendations"])
        assert any("Review downstream" in rec for rec in result["recommendations"])
        assert any("Update data contracts" in rec for rec in result["recommendations"])

    def test_report_with_no_changes(self) -> None:
        """Test reporting when schemas are identical."""
        result = report_schema_differences(
            dataset_a_urn="urn:li:dataset:a",
            dataset_b_urn="urn:li:dataset:b",
            added_fields=[],
            removed_fields=[],
            modified_fields=[],
            breaking_changes=False,
        )

        assert result["changes"]["added_fields"] == []
        assert result["changes"]["removed_fields"] == []
        assert result["changes"]["modified_fields"] == []
        assert result["impact"]["breaking_changes"] is False
        assert result["impact"]["severity"] == "LOW"
        assert len(result["recommendations"]) == 0

    def test_report_structure(self) -> None:
        """Test that report has expected structure."""
        result = report_schema_differences(
            dataset_a_urn="urn:li:dataset:a",
            dataset_b_urn="urn:li:dataset:b",
            added_fields=["field1"],
            removed_fields=[],
            modified_fields=[],
            breaking_changes=False,
        )

        # Verify structure
        assert "comparison" in result
        assert "changes" in result
        assert "impact" in result
        assert "recommendations" in result

        assert "dataset_a" in result["comparison"]
        assert "dataset_b" in result["comparison"]

        assert "added_fields" in result["changes"]
        assert "removed_fields" in result["changes"]
        assert "modified_fields" in result["changes"]

        assert "breaking_changes" in result["impact"]
        assert "severity" in result["impact"]


class TestRespondWithSchemaReport:
    """Test respond_with_schema_report function."""

    def test_respond_with_minimal_report(self) -> None:
        """Test responding with minimal schema report."""
        report = {
            "comparison": {
                "dataset_a": "urn:li:dataset:a",
                "dataset_b": "urn:li:dataset:b",
            },
            "changes": {
                "added_fields": [],
                "removed_fields": [],
                "modified_fields": [],
            },
            "impact": {
                "breaking_changes": False,
                "severity": "LOW",
            },
            "recommendations": [],
        }

        result = respond_with_schema_report(report)

        assert "text" in result
        assert "suggestions" in result
        assert "Schema Comparison Results" in result["text"]
        assert "urn:li:dataset:a" in result["text"]
        assert "urn:li:dataset:b" in result["text"]
        assert "LOW" in result["text"]
        assert isinstance(result["suggestions"], list)

    def test_respond_with_breaking_changes(self) -> None:
        """Test responding with breaking changes."""
        report = {
            "comparison": {
                "dataset_a": "urn:li:dataset:a",
                "dataset_b": "urn:li:dataset:b",
            },
            "changes": {
                "added_fields": ["new_field"],
                "removed_fields": ["old_field"],
                "modified_fields": [{"field_name": "modified_field"}],
            },
            "impact": {
                "breaking_changes": True,
                "severity": "HIGH",
            },
            "recommendations": ["Test recommendation 1", "Test recommendation 2"],
        }

        result = respond_with_schema_report(report)

        assert "Breaking changes detected" in result["text"]
        assert "HIGH" in result["text"]
        assert "Test recommendation 1" in result["text"]
        assert "Test recommendation 2" in result["text"]
        assert "Added Fields: 1" in result["text"]
        assert "Removed Fields: 1" in result["text"]
        assert "Modified Fields: 1" in result["text"]

    def test_respond_with_multiple_recommendations(self) -> None:
        """Test responding with multiple recommendations."""
        report = {
            "comparison": {"dataset_a": "a", "dataset_b": "b"},
            "changes": {
                "added_fields": [],
                "removed_fields": [],
                "modified_fields": [],
            },
            "impact": {"breaking_changes": False, "severity": "MEDIUM"},
            "recommendations": ["Rec 1", "Rec 2", "Rec 3"],
        }

        result = respond_with_schema_report(report)

        assert "Rec 1" in result["text"]
        assert "Rec 2" in result["text"]
        assert "Rec 3" in result["text"]

    def test_respond_formats_as_markdown(self) -> None:
        """Test that response is formatted as markdown."""
        report = {
            "comparison": {"dataset_a": "a", "dataset_b": "b"},
            "changes": {
                "added_fields": [],
                "removed_fields": [],
                "modified_fields": [],
            },
            "impact": {"breaking_changes": False, "severity": "LOW"},
            "recommendations": ["Test"],
        }

        result = respond_with_schema_report(report)

        # Check for markdown formatting
        assert "##" in result["text"]  # Header
        assert "**" in result["text"]  # Bold text
        assert "-" in result["text"]  # List items


class TestCreateSchemaComparisonAgent:
    """Test create_schema_comparison_agent function."""

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_creates_agent_runner(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that function creates an AgentRunner."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_schema_comparison_agent(mock_client)

        assert mock_agent_runner.called

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_agent_has_correct_name(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent is configured with correct name."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_schema_comparison_agent(mock_client)

        # Check that AgentRunner was called with correct config
        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        assert config.agent_name == "Schema Comparison Agent"
        assert "schema" in config.agent_description.lower()

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_agent_has_schema_tools(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent has schema comparison tools."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_schema_comparison_agent(mock_client)

        # Check tool names in config
        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        tool_names = [tool.name for tool in config.tools]
        assert "report_schema_differences" in tool_names
        assert "respond_to_user" in tool_names

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_agent_uses_low_temperature(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent is configured with low temperature for consistency."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_schema_comparison_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        # Schema comparison should use lower temperature for consistent analysis
        assert config.temperature == 0.3

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_agent_with_existing_history(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test creating agent with existing chat history."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}
        history = ChatHistory()

        create_schema_comparison_agent(mock_client, history=history)

        call_args = mock_agent_runner.call_args
        assert call_args[1]["history"] == history

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_agent_has_reasonable_max_tool_calls(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent has appropriate max tool calls limit."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_schema_comparison_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        # Schema comparison is focused, should have reasonable limit
        assert config.max_tool_calls == 15

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_agent_plannable_tools_excludes_respond_to_user(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that plannable tools exclude internal control-flow tools."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_schema_comparison_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        plannable_tool_names = [tool.name for tool in config.plannable_tools]
        assert "respond_to_user" not in plannable_tool_names
        assert "report_schema_differences" in plannable_tool_names

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_agent_uses_correct_model(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent uses specified model."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_schema_comparison_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        assert "claude" in config.model_id.lower()
        assert "sonnet" in config.model_id.lower()


class TestSchemaComparisonAgentIntegration:
    """Integration tests for schema comparison agent."""

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.AgentRunner")
    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_agent_system_prompt_mentions_schema(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that system prompt is appropriate for schema comparison."""
        mock_client = Mock(spec=DataHubClient)
        mock_client._graph = Mock()
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_schema_comparison_agent(mock_client)

        # Get system prompt builder from config
        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        system_messages = config.system_prompt_builder.build_system_messages(
            mock_client
        )

        # Check that prompt mentions schema comparison
        prompt_text = " ".join(msg.get("text", "") for msg in system_messages)
        assert "schema" in prompt_text.lower()
        assert "comparison" in prompt_text.lower() or "compare" in prompt_text.lower()

    @patch("datahub_integrations.chat.examples.schema_comparison_agent.mcp")
    def test_report_schema_differences_generates_all_recommendations(
        self, mock_mcp: Mock
    ) -> None:
        """Test that all possible recommendations can be generated."""
        # Test all recommendation triggers
        result = report_schema_differences(
            dataset_a_urn="a",
            dataset_b_urn="b",
            added_fields=[],
            removed_fields=["field1"],  # Triggers downstream review rec
            modified_fields=[{"field": "field2"}],  # Triggers data contract update rec
            breaking_changes=True,  # Triggers staged rollout rec
        )

        assert len(result["recommendations"]) == 3
        assert any("downstream" in rec.lower() for rec in result["recommendations"])
        assert any("staged rollout" in rec.lower() for rec in result["recommendations"])
        assert any("data contract" in rec.lower() for rec in result["recommendations"])
