"""Tests for plan progress formatting functionality."""

from unittest.mock import MagicMock

import pytest

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentRunner,
    StaticPromptBuilder,
)
from datahub_integrations.chat.planner.models import Constraints, Plan, Step
from datahub_integrations.chat.planner.tools import get_plan_by_id
from datahub_integrations.chat.utils import ParsedReasoning, format_plan_progress


class TestFormatPlanProgress:
    """Tests for the format_plan_progress function."""

    @pytest.fixture
    def mock_agent(self) -> AgentRunner:
        """Create a mock AgentRunner with a sample plan."""
        config = AgentConfig(
            model_id="test-model",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
        )
        client = MagicMock()
        agent = AgentRunner(config=config, client=client)
        return agent

    @pytest.fixture
    def sample_plan(self) -> Plan:
        """Create a sample plan for testing."""
        return Plan(
            plan_id="plan_test123",
            version=1,
            title="Find Affected Dashboards",
            goal="Find all dashboards affected by deprecating a dataset",
            assumptions=["Dataset exists", "User has access"],
            constraints=Constraints(
                tool_allowlist=["search", "get_lineage"],
                max_tool_calls=10,
            ),
            steps=[
                Step(
                    id="s0",
                    description="Find the orders dataset",
                    done_when="Dataset found with exact name match",
                ),
                Step(
                    id="s1",
                    description="Get downstream lineage",
                    done_when="Retrieved all downstream assets",
                ),
                Step(
                    id="s2",
                    description="Filter for Looker dashboards",
                    done_when="Identified all Looker assets",
                ),
                Step(
                    id="s3",
                    description="Compile report",
                    done_when="Report generated with all affected dashboards",
                ),
            ],
            expected_deliverable="List of affected Looker dashboards",
        )

    def test_format_plan_progress_with_current_step_at_beginning(
        self, mock_agent: AgentRunner, sample_plan: Plan
    ) -> None:
        """Test format_plan_progress with current step at the beginning."""
        mock_agent.plan_cache["plan_test123"] = {"plan": sample_plan, "progress": {}}

        result = format_plan_progress(
            plan_id="plan_test123",
            current_step_id="s0",
            step_status="in_progress",
            agent=mock_agent,
            reasoning_message="Searching for the dataset",
        )

        assert "**Plan: Find Affected Dashboards**" in result
        assert "▶ Find the orders dataset" in result
        assert (
            "> _Searching for the dataset_" in result
        )  # Blockquote indented and italic
        assert "• Get downstream lineage" in result
        assert "• Filter for Looker dashboards" in result
        assert "• Compile report" in result
        # Should use double newlines
        assert "\n\n" in result

    def test_format_plan_progress_with_current_step_in_middle(
        self, mock_agent: AgentRunner, sample_plan: Plan
    ) -> None:
        """Test format_plan_progress with current step in the middle."""
        mock_agent.plan_cache["plan_test123"] = {"plan": sample_plan, "progress": {}}

        result = format_plan_progress(
            plan_id="plan_test123",
            current_step_id="s2",
            step_status="in_progress",
            agent=mock_agent,
            reasoning_message="Filtering Looker assets",
        )

        assert "✓ Find the orders dataset" in result
        assert "✓ Get downstream lineage" in result
        assert "▶ Filter for Looker dashboards" in result
        assert "> _Filtering Looker assets_" in result
        assert "• Compile report" in result

    def test_format_plan_progress_with_current_step_at_end(
        self, mock_agent: AgentRunner, sample_plan: Plan
    ) -> None:
        """Test format_plan_progress with current step at the end."""
        mock_agent.plan_cache["plan_test123"] = {"plan": sample_plan, "progress": {}}

        result = format_plan_progress(
            plan_id="plan_test123",
            current_step_id="s3",
            step_status="in_progress",
            agent=mock_agent,
            reasoning_message="Generating final report",
        )

        assert "✓ Find the orders dataset" in result
        assert "✓ Get downstream lineage" in result
        assert "✓ Filter for Looker dashboards" in result
        assert "▶ Compile report" in result
        assert "> _Generating final report_" in result

    def test_format_plan_progress_with_no_current_step(
        self, mock_agent: AgentRunner, sample_plan: Plan
    ) -> None:
        """Test format_plan_progress with no current step (all pending)."""
        mock_agent.plan_cache["plan_test123"] = {"plan": sample_plan, "progress": {}}

        result = format_plan_progress(
            plan_id="plan_test123",
            current_step_id=None,
            step_status=None,
            agent=mock_agent,
            reasoning_message="Plan created",
        )

        # All steps should be pending
        assert "• Find the orders dataset" in result
        assert "• Get downstream lineage" in result
        assert "• Filter for Looker dashboards" in result
        assert "• Compile report" in result
        # No checkmarks or arrows
        assert "✓" not in result
        assert "▶" not in result

    def test_format_plan_progress_with_invalid_step_id(
        self, mock_agent: AgentRunner, sample_plan: Plan
    ) -> None:
        """Test format_plan_progress with hallucinated/invalid step ID (graceful degradation)."""
        mock_agent.plan_cache["plan_test123"] = {"plan": sample_plan, "progress": {}}

        result = format_plan_progress(
            plan_id="plan_test123",
            current_step_id="s999",  # Invalid step ID
            step_status="in_progress",
            agent=mock_agent,
            reasoning_message="Processing",
        )

        # Should treat all steps as pending (graceful fallback)
        assert "• Find the orders dataset" in result
        assert "• Get downstream lineage" in result
        assert "• Filter for Looker dashboards" in result
        assert "• Compile report" in result

    def test_format_plan_progress_plan_not_found(self, mock_agent: AgentRunner) -> None:
        """Test format_plan_progress when plan is not found in cache."""
        result = format_plan_progress(
            plan_id="plan_nonexistent",
            current_step_id="s0",
            step_status="in_progress",
            agent=mock_agent,
            reasoning_message="Just the reasoning",
        )

        # Should return just the reasoning message
        assert result == "Just the reasoning"

    def test_format_plan_progress_with_empty_reasoning_message(
        self, mock_agent: AgentRunner, sample_plan: Plan
    ) -> None:
        """Test format_plan_progress with empty reasoning message."""
        mock_agent.plan_cache["plan_test123"] = {"plan": sample_plan, "progress": {}}

        result = format_plan_progress(
            plan_id="plan_test123",
            current_step_id="s1",
            step_status="in_progress",
            agent=mock_agent,
            reasoning_message="",
        )

        assert "▶ Get downstream lineage" in result
        # Empty reasoning message should not add blockquote line
        assert "> _" not in result

    def test_format_plan_progress_with_single_step_plan(
        self, mock_agent: AgentRunner
    ) -> None:
        """Test format_plan_progress with a plan containing only one step."""
        single_step_plan = Plan(
            plan_id="plan_single",
            version=1,
            title="Simple Task",
            goal="Do one thing",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=5),
            steps=[
                Step(
                    id="s0",
                    description="Execute the task",
                    done_when="Task completed",
                )
            ],
            expected_deliverable="Task result",
        )
        mock_agent.plan_cache["plan_single"] = {
            "plan": single_step_plan,
            "progress": {},
        }

        result = format_plan_progress(
            plan_id="plan_single",
            current_step_id="s0",
            step_status="in_progress",
            agent=mock_agent,
            reasoning_message="Working on it",
        )

        assert "**Plan: Simple Task**" in result
        assert "▶ Execute the task" in result
        assert "> _Working on it_" in result

    def test_format_plan_progress_with_special_characters_in_description(
        self, mock_agent: AgentRunner
    ) -> None:
        """Test format_plan_progress with special characters in step descriptions."""
        special_plan = Plan(
            plan_id="plan_special",
            version=1,
            title="Plan with Special Chars & Symbols",
            goal="Test special characters",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=5),
            steps=[
                Step(
                    id="s0",
                    description="Search for 'datasets' with @symbols",
                    done_when="Found",
                ),
                Step(
                    id="s1",
                    description="Process results: 100% complete!",
                    done_when="Done",
                ),
            ],
            expected_deliverable="Results",
        )
        mock_agent.plan_cache["plan_special"] = {
            "plan": special_plan,
            "progress": {},
        }

        result = format_plan_progress(
            plan_id="plan_special",
            current_step_id="s0",
            step_status="in_progress",
            agent=mock_agent,
            reasoning_message="Processing...",
        )

        assert "▶ Search for 'datasets' with @symbols" in result
        assert "> _Processing..._" in result
        assert "• Process results: 100% complete!" in result


class TestGetPlanById:
    """Tests for the get_plan_by_id function."""

    @pytest.fixture
    def mock_agent(self) -> AgentRunner:
        """Create a mock AgentRunner."""
        config = AgentConfig(
            model_id="test-model",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
        )
        client = MagicMock()
        agent = AgentRunner(config=config, client=client)
        return agent

    def test_get_plan_by_id_plan_found(self, mock_agent: AgentRunner) -> None:
        """Test get_plan_by_id returns plan when it exists in cache."""
        plan = Plan(
            plan_id="plan_exists",
            version=1,
            title="Test Plan",
            goal="Test",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=5),
            steps=[],
            expected_deliverable="Result",
        )
        mock_agent.plan_cache["plan_exists"] = {"plan": plan, "progress": {}}

        result = get_plan_by_id("plan_exists", mock_agent)

        assert result is not None
        assert result.plan_id == "plan_exists"
        assert result.title == "Test Plan"

    def test_get_plan_by_id_plan_not_found(self, mock_agent: AgentRunner) -> None:
        """Test get_plan_by_id returns None when plan doesn't exist."""
        result = get_plan_by_id("plan_nonexistent", mock_agent)

        assert result is None

    def test_get_plan_by_id_empty_cache(self, mock_agent: AgentRunner) -> None:
        """Test get_plan_by_id with empty cache."""
        mock_agent.plan_cache = {}

        result = get_plan_by_id("any_plan_id", mock_agent)

        assert result is None


class TestParsedReasoningWithPlanProgress:
    """Tests for ParsedReasoning.to_user_visible_message with plan progress."""

    @pytest.fixture
    def mock_agent(self) -> AgentRunner:
        """Create a mock AgentRunner with a sample plan."""
        config = AgentConfig(
            model_id="test-model",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
        )
        client = MagicMock()
        agent = AgentRunner(config=config, client=client)
        plan = Plan(
            plan_id="plan_abc",
            version=1,
            title="Test Plan",
            goal="Complete the task",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=5),
            steps=[
                Step(id="s0", description="Step 1", done_when="Done"),
                Step(id="s1", description="Step 2", done_when="Done"),
            ],
            expected_deliverable="Result",
        )
        agent.plan_cache = {"plan_abc": {"plan": plan, "progress": {}}}
        return agent

    def test_to_user_visible_message_with_plan_shows_progress(
        self, mock_agent: AgentRunner
    ) -> None:
        """Test that to_user_visible_message shows plan progress when plan_id present."""
        parsed = ParsedReasoning(
            action="Execute step",
            plan_id="plan_abc",
            plan_step="s0",
            step_status="in_progress",
            raw_text="<reasoning>test</reasoning>",
        )

        result = parsed.to_user_visible_message(session=mock_agent)

        # Should show plan progress, not just the action
        assert "**Plan: Test Plan**" in result
        assert "▶ Step 1" in result
        assert "• Step 2" in result

    def test_to_user_visible_message_with_plan_id_but_no_session(self) -> None:
        """Test that to_user_visible_message falls back when plan_id present but no session."""
        parsed = ParsedReasoning(
            action="Execute step",
            plan_id="plan_abc",
            plan_step="s0",
            step_status="in_progress",
            raw_text="<reasoning>test</reasoning>",
        )

        result = parsed.to_user_visible_message(session=None)

        # Should fall back to normal formatting (no plan progress)
        assert result == "Execute step"
        assert "**Plan:" not in result

    def test_to_user_visible_message_without_plan_id_uses_normal_formatting(
        self, mock_agent: AgentRunner
    ) -> None:
        """Test that to_user_visible_message uses normal formatting when no plan_id."""
        parsed = ParsedReasoning(
            action="Search for dataset",
            confidence="high",
            raw_text="<reasoning>test</reasoning>",
        )

        result = parsed.to_user_visible_message(session=mock_agent)

        # Should use normal formatting
        assert result == "Search for dataset"
        assert "**Plan:" not in result

    def test_to_user_visible_message_with_plan_includes_warnings(
        self, mock_agent: AgentRunner
    ) -> None:
        """Test that warnings are preserved in plan progress display."""
        parsed = ParsedReasoning(
            action="Execute step",
            warning="Found 100+ results",
            plan_id="plan_abc",
            plan_step="s0",
            step_status="in_progress",
            raw_text="<reasoning>test</reasoning>",
        )

        result = parsed.to_user_visible_message(session=mock_agent)

        # Plan progress should be shown with warning in reasoning
        assert "**Plan: Test Plan**" in result
        # Warning should appear as part of the reasoning message under current step
        assert "⚠️" in result or "Found 100+ results" in result
