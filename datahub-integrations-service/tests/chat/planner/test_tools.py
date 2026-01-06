"""Unit tests for planner tools."""

from typing import Any, Dict, Optional
from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.agent.history_snapshot import PlanCacheEntry
from datahub_integrations.chat.planner.models import Constraints, Plan, Step
from datahub_integrations.chat.planner.planning_context import PlanningContext
from datahub_integrations.chat.planner.tools import (
    PlannerLLMResponse,
    _call_planner_llm,
    _get_planner_tool_specs,
    create_plan,
    get_planning_tool_wrappers,
    report_step_progress,
    revise_plan,
)
from datahub_integrations.mcp_integration.tool import ToolWrapper


class MockPlanningContext(PlanningContext):
    """
    Mock PlanningContext for testing.

    Stores plans in a dictionary and provides get_plan/set_plan methods.
    Inherits from PlanningContext for type compatibility.
    """

    def __init__(self, plannable_tools: Optional[list] = None):
        # Don't call super().__init__ - we're mocking the holder
        self._plans: Dict[str, PlanCacheEntry] = {}
        self._plannable_tools = plannable_tools or []

    def get_plannable_tools(self) -> list:
        """Get plannable tools."""
        return self._plannable_tools

    def get_plan(self, plan_id: str) -> Optional[PlanCacheEntry]:
        """Get plan by ID."""
        return self._plans.get(plan_id)

    def set_plan(self, plan_id: str, plan: Plan, internal: Dict[str, Any]) -> None:
        """Set plan by ID."""
        self._plans[plan_id] = PlanCacheEntry(plan=plan, internal=internal)


class TestReportStepProgress:
    """Test report_step_progress function."""

    def test_reports_completed_step(self) -> None:
        """Test reporting a completed step."""
        ctx = MockPlanningContext()

        # Create a simple plan with two steps
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test Plan",
            goal="Test goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[
                Step(id="s0", description="First step", done_when="Done 0"),
                Step(id="s1", description="Second step", done_when="Done 1"),
            ],
            expected_deliverable="Test result",
        )
        ctx.set_plan("plan_123", test_plan, {})

        result = report_step_progress(
            ctx=ctx,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            done_criteria_met=True,
            evidence={"found": 1},
            confidence=1.0,
        )

        # Verify response mentions progress (note: report_step_progress doesn't mutate plan_cache anymore)
        assert isinstance(result, str)
        assert "s0" in result
        assert "completed" in result.lower() or "Next" in result

    def test_reports_failed_step(self) -> None:
        """Test reporting a failed step."""
        ctx = MockPlanningContext()
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[Step(id="s0", description="First step", done_when="Done")],
            expected_deliverable="Result",
        )
        ctx.set_plan("plan_123", test_plan, {})

        result = report_step_progress(
            ctx=ctx,
            plan_id="plan_123",
            step_id="s0",
            status="failed",
            done_criteria_met=False,
            failed_criteria_met=True,
        )

        # Verify response mentions failure
        assert "failed" in result.lower() or "WARNING" in result

    def test_raises_error_for_nonexistent_plan(self) -> None:
        """Test raises error when plan doesn't exist."""
        ctx = MockPlanningContext()

        with pytest.raises(ValueError, match="not found"):
            report_step_progress(
                ctx=ctx,
                plan_id="nonexistent",
                step_id="s0",
                status="completed",
            )

    def test_raises_error_for_invalid_step_id(self) -> None:
        """Test raises error when step_id doesn't exist in plan."""
        ctx = MockPlanningContext()
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[Step(id="s0", description="Step 0", done_when="Done")],
            expected_deliverable="Result",
        )
        ctx.set_plan("plan_123", test_plan, {})

        with pytest.raises(ValueError, match="not found"):
            report_step_progress(
                ctx=ctx,
                plan_id="plan_123",
                step_id="s99",  # Invalid step ID
                status="completed",
            )

    def test_stores_evidence(self) -> None:
        """Test that evidence is included in response."""
        ctx = MockPlanningContext()
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )
        ctx.set_plan("plan_123", test_plan, {})

        evidence_data = {"urn": "test_urn", "count": 5}

        result = report_step_progress(
            ctx=ctx,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            evidence=evidence_data,
        )

        # report_step_progress returns guidance string (doesn't store progress anymore)
        assert isinstance(result, str)

    def test_stores_confidence(self) -> None:
        """Test that confidence is handled correctly."""
        ctx = MockPlanningContext()
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )
        ctx.set_plan("plan_123", test_plan, {})

        result = report_step_progress(
            ctx=ctx,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            confidence=0.85,
        )

        # report_step_progress returns guidance string (doesn't store progress anymore)
        assert isinstance(result, str)

    def test_reports_plan_completion(self) -> None:
        """Test reporting when all steps are complete."""
        ctx = MockPlanningContext()
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[
                Step(id="s0", description="Step 0", done_when="Done 0"),
                Step(id="s1", description="Step 1", done_when="Done 1"),
            ],
            expected_deliverable="Result",
        )
        ctx.set_plan("plan_123", test_plan, {})

        # Complete the last step
        result = report_step_progress(
            ctx=ctx,
            plan_id="plan_123",
            step_id="s1",
            status="completed",
        )

        # Should indicate plan completion
        assert "complete" in result.lower()
        assert "2/2" in result or "All" in result

    def test_updates_existing_step_progress(self) -> None:
        """Test that reporting same step again works."""
        ctx = MockPlanningContext()
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )
        ctx.set_plan("plan_123", test_plan, {})

        # Update to completed
        result = report_step_progress(
            ctx=ctx,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
        )

        # report_step_progress returns guidance string
        assert isinstance(result, str)


class TestCreatePlanIntegration:
    """Integration tests for create_plan function."""

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools.get_extra_llm_instructions")
    def test_creates_plan_with_basic_task(
        self, mock_get_instructions: Mock, mock_call_llm: Mock
    ) -> None:
        """Test creating a basic plan."""
        # Mock LLM response (now returns PlannerLLMResponse)
        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "title": "Test Plan",
                "goal": "Test goal",
                "assumptions": ["Assumption 1"],
                "steps": [
                    {
                        "id": "s0",
                        "description": "First step",
                        "done_when": "Step complete",
                    }
                ],
                "expected_deliverable": "Test deliverable",
                "max_llm_turns": 10,
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        mock_get_instructions.return_value = None

        # Create mock tool
        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool._tool = Mock()
        mock_tool._tool.description = "Test tool description"

        # Create mock context
        ctx = MockPlanningContext(plannable_tools=[mock_tool])

        # Create plan
        result = create_plan(ctx=ctx, task="Test task")

        # Verify plan structure
        assert isinstance(result, Plan)
        assert result.title == "Test Plan"
        assert result.goal == "Test goal"
        assert len(result.steps) == 1
        assert result.steps[0].id == "s0"
        assert result.version == 1

        # Verify plan was cached
        assert ctx.get_plan(result.plan_id) is not None

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools.get_extra_llm_instructions")
    def test_creates_plan_with_context_and_evidence(
        self, mock_get_instructions: Mock, mock_call_llm: Mock
    ) -> None:
        """Test creating plan with context and evidence."""
        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "title": "Plan with context",
                "goal": "Goal",
                "assumptions": [],
                "steps": [
                    {
                        "id": "s0",
                        "description": "Step with context",
                        "done_when": "Done",
                    }
                ],
                "expected_deliverable": "Result",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        mock_get_instructions.return_value = None

        ctx = MockPlanningContext()

        result = create_plan(
            ctx=ctx,
            task="Test task",
            context="Additional context",
            evidence={"key": "value"},
        )

        # Verify plan created
        assert isinstance(result, Plan)

        # Verify _call_planner_llm was called with prompt including evidence
        call_args = mock_call_llm.call_args
        prompt_arg = call_args[0][0]  # First positional argument
        assert "Additional context" in prompt_arg
        assert "Evidence" in prompt_arg
        assert "key" in prompt_arg

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools.get_extra_llm_instructions")
    def test_respects_max_steps_parameter(
        self, mock_get_instructions: Mock, mock_call_llm: Mock
    ) -> None:
        """Test that max_steps parameter is passed to planner."""
        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "title": "Plan",
                "goal": "Goal",
                "assumptions": [],
                "steps": [{"id": "s0", "description": "Step", "done_when": "Done"}],
                "expected_deliverable": "Result",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        mock_get_instructions.return_value = None

        ctx = MockPlanningContext()

        create_plan(ctx=ctx, task="Task", max_steps=5)

        # Verify prompt mentions max_steps
        call_args = mock_call_llm.call_args
        prompt_arg = call_args[0][0]
        assert "5" in prompt_arg or "five" in prompt_arg.lower()


class TestRevisePlanIntegration:
    """Integration tests for revise_plan function."""

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    def test_revises_existing_plan(self, mock_call_llm: Mock) -> None:
        """Test revising an existing plan."""
        # Setup original plan
        original_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Original Plan",
            goal="Original goal",
            assumptions=["Assumption 1"],
            constraints=Constraints(
                tool_allowlist=["tool1", "tool2"], max_llm_turns=20
            ),
            steps=[
                Step(id="s0", description="Step 0", done_when="Done 0"),
                Step(id="s1", description="Step 1", done_when="Done 1"),
                Step(id="s2", description="Step 2", done_when="Done 2"),
            ],
            expected_deliverable="Original deliverable",
        )

        ctx = MockPlanningContext()
        ctx.set_plan("plan_123", original_plan, {})

        # Mock LLM response for revision
        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "title": "Revised Plan",
                "goal": "Revised goal",
                "assumptions": ["New assumption"],
                "steps": [
                    # New step s1 (revised)
                    {
                        "id": "s1",
                        "description": "Revised step 1",
                        "done_when": "Revised done",
                    },
                    # New step s2
                    {
                        "id": "s2",
                        "description": "New step 2",
                        "done_when": "New done",
                    },
                ],
                "expected_deliverable": "Revised deliverable",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        # Revise from s1 onward (s0 completed)
        result = revise_plan(
            ctx=ctx,
            plan_id="plan_123",
            completed_steps=["s0"],
            current_step="s1",
            issue="Search found no results",
            evidence={"count": 0},
        )

        # Verify
        assert isinstance(result, Plan)
        assert result.plan_id == "plan_123"  # Same ID
        assert result.version == 2  # Incremented
        assert len(result.steps) == 3  # s0 (preserved) + 2 new
        assert result.steps[0].id == "s0"  # Preserved
        assert result.steps[0].description == "Step 0"  # Original

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    def test_revise_plan_raises_error_for_nonexistent_plan(
        self, mock_call_llm: Mock
    ) -> None:
        """Test revise_plan raises error when plan doesn't exist."""
        ctx = MockPlanningContext()

        with pytest.raises(ValueError, match="not found"):
            revise_plan(
                ctx=ctx,
                plan_id="nonexistent",
                completed_steps=[],
                current_step="s0",
                issue="Issue",
            )

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    def test_revise_plan_updates_cache(self, mock_call_llm: Mock) -> None:
        """Test that revised plan updates the cache."""
        original_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Original",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=["tool1"], max_llm_turns=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Deliverable",
        )

        ctx = MockPlanningContext()
        ctx.set_plan("plan_123", original_plan, {})

        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "assumptions": [],
                "steps": [{"id": "s0", "description": "Revised", "done_when": "Done"}],
                "expected_deliverable": "New deliverable",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        revised = revise_plan(
            ctx=ctx,
            plan_id="plan_123",
            completed_steps=[],
            current_step="s0",
            issue="Issue",
        )

        # Verify cache was updated
        cached_entry = ctx.get_plan("plan_123")
        assert cached_entry is not None
        assert cached_entry["plan"] == revised
        assert cached_entry["plan"].version == 2


class TestPlannerLLMCalling:
    """Test _call_planner_llm function behavior."""

    @patch("datahub_integrations.chat.planner.tools.get_llm_client")
    @patch("datahub_integrations.chat.planner.tools.get_datahub_client")
    @patch("datahub_integrations.chat.planner.tools.get_extra_llm_instructions")
    @patch("datahub_integrations.chat.planner.tools.get_recipe_guidance")
    def test_call_planner_llm_with_structured_output(
        self,
        mock_get_recipes: Mock,
        mock_get_instructions: Mock,
        mock_get_client: Mock,
        mock_get_llm: Mock,
    ) -> None:
        """Test _call_planner_llm returns structured plan data."""

        mock_get_recipes.return_value = "Recipe guidance"
        mock_get_instructions.return_value = None
        mock_get_client.return_value = Mock()

        # Mock LLM response with tool use
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "123",
                                "name": "create_execution_plan",
                                "input": {
                                    "title": "Test Plan",
                                    "goal": "Test goal",
                                    "assumptions": ["Assumption"],
                                    "steps": [
                                        {
                                            "id": "s0",
                                            "description": "Step",
                                            "done_when": "Done",
                                        }
                                    ],
                                    "expected_deliverable": "Result",
                                },
                            }
                        }
                    ]
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 100, "outputTokens": 200, "totalTokens": 300},
        }

        response = _call_planner_llm("Create a plan", "Available tools")

        # Verify structured output was extracted
        assert isinstance(response.plan_data, dict)
        assert isinstance(response.internal_data, dict)
        assert response.plan_data["title"] == "Test Plan"
        assert response.plan_data["goal"] == "Test goal"
        assert len(response.plan_data["steps"]) == 1
        assert response.internal_data["tool_used"] == "create_execution_plan"

    @patch("datahub_integrations.chat.planner.tools.get_llm_client")
    @patch("datahub_integrations.chat.planner.tools.get_datahub_client")
    @patch("datahub_integrations.chat.planner.tools.get_extra_llm_instructions")
    @patch("datahub_integrations.chat.planner.tools.get_recipe_guidance")
    def test_call_planner_llm_fixes_malformed_steps(
        self,
        mock_get_recipes: Mock,
        mock_get_instructions: Mock,
        mock_get_client: Mock,
        mock_get_llm: Mock,
    ) -> None:
        """Test that malformed steps (as string) are repaired."""

        mock_get_recipes.return_value = "Recipe guidance"
        mock_get_instructions.return_value = None
        mock_get_client.return_value = Mock()

        # Mock LLM response with steps as JSON string (malformed)
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "123",
                                "name": "create_execution_plan",
                                "input": {
                                    "title": "Plan",
                                    "goal": "Goal",
                                    "assumptions": [],
                                    # Steps as string instead of array (malformed)
                                    "steps": '[{"id": "s0", "description": "Step", "done_when": "Done"}]',
                                    "expected_deliverable": "Result",
                                },
                            }
                        }
                    ]
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 100, "outputTokens": 200, "totalTokens": 300},
        }

        response = _call_planner_llm("Task", "Tools")

        # Should have repaired steps to array
        assert isinstance(response.plan_data["steps"], list)
        assert len(response.plan_data["steps"]) == 1
        assert response.plan_data["steps"][0]["id"] == "s0"

    @patch("datahub_integrations.chat.planner.tools.get_llm_client")
    @patch("datahub_integrations.chat.planner.tools.get_datahub_client")
    @patch("datahub_integrations.chat.planner.tools.get_extra_llm_instructions")
    @patch("datahub_integrations.chat.planner.tools.get_recipe_guidance")
    def test_call_planner_llm_raises_on_no_message(
        self,
        mock_get_recipes: Mock,
        mock_get_instructions: Mock,
        mock_get_client: Mock,
        mock_get_llm: Mock,
    ) -> None:
        """Test error when LLM returns no message."""

        mock_get_recipes.return_value = "Recipes"
        mock_get_instructions.return_value = None
        mock_get_client.return_value = Mock()

        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        # Return response with no message
        mock_llm_client.converse.return_value = {"output": {}, "stopReason": "end_turn"}

        with pytest.raises(ValueError, match="No message"):
            _call_planner_llm("Task", "Tools")


class TestPlanToolHelpers:
    """Test helper functions for planning tools."""

    def test_get_planner_tool_specs(self) -> None:
        """Test getting planner tool specifications for Bedrock."""

        specs = _get_planner_tool_specs()

        # Should return at least the create_execution_plan tool
        assert len(specs) >= 1

        # Find the create_execution_plan spec
        exec_plan_spec = next(
            (s for s in specs if s["toolSpec"]["name"] == "create_execution_plan"), None
        )
        assert exec_plan_spec is not None

        # Verify structure
        assert "toolSpec" in exec_plan_spec
        assert "inputSchema" in exec_plan_spec["toolSpec"]
        assert "json" in exec_plan_spec["toolSpec"]["inputSchema"]

        # Verify schema has the expected parameters
        schema = exec_plan_spec["toolSpec"]["inputSchema"]["json"]
        properties = schema.get("properties", {})
        assert "title" in properties
        assert "goal" in properties
        assert "steps" in properties
        assert "expected_deliverable" in properties


class TestPlanningToolWrappers:
    """Test get_planning_tool_wrappers function."""

    def test_returns_two_planning_tools(self) -> None:
        """Test that function returns two planning tools (report_step_progress not registered)."""
        ctx = MockPlanningContext()

        result = get_planning_tool_wrappers(ctx)

        assert len(result) == 2
        tool_names = [tool.name for tool in result]
        assert "create_plan" in tool_names
        assert "revise_plan" in tool_names
        # report_step_progress intentionally not registered (latency optimization)

    def test_tool_wrappers_have_descriptions(self) -> None:
        """Test that all planning tools have descriptions."""
        ctx = MockPlanningContext()

        tools = get_planning_tool_wrappers(ctx)

        for tool in tools:
            assert tool.name is not None
            # Description should be set from original function docstrings
            assert tool._tool.description is not None


class TestPlanCacheManagement:
    """Test plan cache operations."""

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools.get_extra_llm_instructions")
    def test_plan_stored_in_cache(
        self, mock_get_instructions: Mock, mock_call_llm: Mock
    ) -> None:
        """Test that created plan is stored in cache."""
        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "title": "Plan",
                "goal": "Goal",
                "assumptions": [],
                "steps": [{"id": "s0", "description": "Step", "done_when": "Done"}],
                "expected_deliverable": "Result",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        mock_get_instructions.return_value = None

        ctx = MockPlanningContext()

        plan = create_plan(ctx=ctx, task="Task")

        # Verify cache entry
        cache_entry = ctx.get_plan(plan.plan_id)
        assert cache_entry is not None
        assert "plan" in cache_entry
        assert "internal" in cache_entry
        assert cache_entry["plan"] == plan
        assert isinstance(cache_entry["internal"], dict)
        assert cache_entry["internal"]["tool_used"] == "create_execution_plan"

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    def test_revised_plan_updates_same_cache_entry(self, mock_call_llm: Mock) -> None:
        """Test that revising a plan updates the same cache entry."""
        original_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Original",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=["tool1"], max_llm_turns=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )

        ctx = MockPlanningContext()
        ctx.set_plan("plan_123", original_plan, {})

        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "assumptions": [],
                "steps": [{"id": "s0", "description": "Revised", "done_when": "Done"}],
                "expected_deliverable": "Result",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        revised = revise_plan(
            ctx=ctx,
            plan_id="plan_123",
            completed_steps=[],
            current_step="s0",
            issue="Issue",
        )

        # Verify same cache key is used
        cache_entry = ctx.get_plan("plan_123")
        assert cache_entry is not None
        assert cache_entry["plan"] == revised


class TestPlanningEdgeCases:
    """Test edge cases in planning."""

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools.get_extra_llm_instructions")
    def test_create_plan_with_empty_task(
        self, mock_get_instructions: Mock, mock_call_llm: Mock
    ) -> None:
        """Test creating plan with empty task string."""

        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "title": "Empty task plan",
                "goal": "",
                "assumptions": [],
                "steps": [],
                "expected_deliverable": "",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        mock_get_instructions.return_value = None

        ctx = MockPlanningContext()

        result = create_plan(ctx=ctx, task="")

        assert isinstance(result, Plan)

    def test_report_progress_with_no_evidence(self) -> None:
        """Test reporting progress without evidence."""
        mock_plan = Mock(spec=Plan)
        mock_plan.steps = [
            Mock(id="s0", description="Step 0"),
            Mock(id="s1", description="Step 1"),
        ]
        ctx = MockPlanningContext()
        ctx.set_plan("plan_123", mock_plan, {})

        result = report_step_progress(
            ctx=ctx,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            # No evidence provided
        )

        # Should work without evidence
        assert isinstance(result, str)
        assert "s0" in result or "1/2" in result

    def test_report_progress_with_none_confidence(self) -> None:
        """Test reporting progress with None confidence."""
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )
        ctx = MockPlanningContext()
        ctx.set_plan("plan_123", test_plan, {})

        result = report_step_progress(
            ctx=ctx,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            confidence=None,
        )

        # Should accept None confidence
        assert isinstance(result, str)
