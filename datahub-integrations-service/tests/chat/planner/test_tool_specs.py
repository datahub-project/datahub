"""Tests to verify planning tool specs hide session parameter from LLM."""

from unittest.mock import MagicMock

import pytest

from datahub_integrations.chat.chat_session import ChatSession
from datahub_integrations.chat.planner.tools import get_planning_tool_wrappers


class TestPlanningToolSpecs:
    """Tests for planning tool specifications (MCP tool specs)."""

    @pytest.fixture
    def mock_session(self) -> ChatSession:
        """Create a mock ChatSession for testing tool wrappers."""
        # Create a minimal mock session with required attributes
        session = MagicMock(spec=ChatSession)
        session.plan_cache = {}
        session.session_id = "test_session_123"
        return session

    def test_create_plan_spec_hides_session_parameter(
        self, mock_session: ChatSession
    ) -> None:
        """Test that create_plan tool spec does not expose session parameter to LLM."""
        wrappers = get_planning_tool_wrappers(mock_session)

        # Find create_plan wrapper
        create_plan_wrapper = next(
            (w for w in wrappers if w.name == "create_plan"), None
        )
        assert create_plan_wrapper is not None, "create_plan tool wrapper not found"

        # Get the Bedrock tool spec that the LLM will see
        spec = create_plan_wrapper.to_bedrock_spec()

        # Extract parameter names from the JSON schema
        input_schema = spec["toolSpec"]["inputSchema"]["json"]
        assert "properties" in input_schema, "Tool spec should have properties"

        parameters = input_schema["properties"].keys()

        # Verify session is NOT in parameters
        assert "session" not in parameters, (
            "session parameter should not be exposed to LLM"
        )

        # Verify expected parameters ARE present
        assert "task" in parameters, "task parameter should be present"
        assert "context" in parameters, "context parameter should be present"
        assert "evidence" in parameters, "evidence parameter should be present"
        assert "max_steps" in parameters, "max_steps parameter should be present"

    def test_revise_plan_spec_hides_session_parameter(
        self, mock_session: ChatSession
    ) -> None:
        """Test that revise_plan tool spec does not expose session parameter to LLM."""
        wrappers = get_planning_tool_wrappers(mock_session)

        # Find revise_plan wrapper
        revise_plan_wrapper = next(
            (w for w in wrappers if w.name == "revise_plan"), None
        )
        assert revise_plan_wrapper is not None, "revise_plan tool wrapper not found"

        # Get the Bedrock tool spec
        spec = revise_plan_wrapper.to_bedrock_spec()
        input_schema = spec["toolSpec"]["inputSchema"]["json"]
        parameters = input_schema["properties"].keys()

        # Verify session is NOT in parameters
        assert "session" not in parameters, (
            "session parameter should not be exposed to LLM"
        )

        # Verify expected parameters ARE present
        assert "plan_id" in parameters
        assert "completed_steps" in parameters
        assert "current_step" in parameters
        assert "issue" in parameters
        assert "evidence" in parameters

    def test_report_step_progress_spec_hides_session_parameter(
        self, mock_session: ChatSession
    ) -> None:
        """Test that report_step_progress tool spec does not expose session parameter to LLM."""
        wrappers = get_planning_tool_wrappers(mock_session)

        # Find report_step_progress wrapper
        report_progress_wrapper = next(
            (w for w in wrappers if w.name == "report_step_progress"), None
        )
        assert report_progress_wrapper is not None, (
            "report_step_progress tool wrapper not found"
        )

        # Get the Bedrock tool spec
        spec = report_progress_wrapper.to_bedrock_spec()
        input_schema = spec["toolSpec"]["inputSchema"]["json"]
        parameters = input_schema["properties"].keys()

        # Verify session is NOT in parameters
        assert "session" not in parameters, (
            "session parameter should not be exposed to LLM"
        )

        # Verify expected parameters ARE present
        assert "plan_id" in parameters
        assert "step_id" in parameters
        assert "status" in parameters
        assert "done_criteria_met" in parameters
        assert "failed_criteria_met" in parameters
        assert "return_to_user_criteria_met" in parameters
        assert "evidence" in parameters
        assert "confidence" in parameters

    def test_all_planning_tools_have_descriptions(
        self, mock_session: ChatSession
    ) -> None:
        """Test that all planning tools have descriptions."""
        wrappers = get_planning_tool_wrappers(mock_session)

        assert len(wrappers) == 3, "Should have exactly 3 planning tools"

        for wrapper in wrappers:
            spec = wrapper.to_bedrock_spec()
            description = spec["toolSpec"]["description"]

            assert description, f"Tool {wrapper.name} should have a description"
            assert len(description) > 50, (
                f"Tool {wrapper.name} description should be substantial"
            )

    def test_planning_tools_have_correct_names(self, mock_session: ChatSession) -> None:
        """Test that planning tools have the expected names."""
        wrappers = get_planning_tool_wrappers(mock_session)

        tool_names = {w.name for w in wrappers}
        expected_names = {"create_plan", "revise_plan", "report_step_progress"}

        assert tool_names == expected_names, (
            f"Expected tools {expected_names}, got {tool_names}"
        )

    def test_parameter_types_preserved(self, mock_session: ChatSession) -> None:
        """Test that parameter type hints are correctly preserved after partial binding."""
        wrappers = get_planning_tool_wrappers(mock_session)

        create_plan_wrapper = next(
            (w for w in wrappers if w.name == "create_plan"), None
        )
        assert create_plan_wrapper is not None

        spec = create_plan_wrapper.to_bedrock_spec()
        input_schema = spec["toolSpec"]["inputSchema"]["json"]
        properties = input_schema["properties"]

        # Check that task is a string type
        assert properties["task"]["type"] == "string", (
            "task parameter should be string type"
        )

        # Check that max_steps is an integer type
        assert properties["max_steps"]["type"] == "integer", (
            "max_steps parameter should be integer type"
        )
