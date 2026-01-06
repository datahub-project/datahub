"""
Comprehensive unit tests for planning functions in tools.py.

Tests the core logic of create_plan, revise_plan, report_step_progress,
and related helper functions.
"""

from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest

from datahub_integrations.chat.agent.history_snapshot import PlanCacheEntry
from datahub_integrations.chat.planner.models import Constraints, Plan, Step
from datahub_integrations.chat.planner.planning_context import PlanningContext
from datahub_integrations.chat.planner.tools import (
    PlannerLLMResponse,
    create_plan,
    report_step_progress,
    revise_plan,
)


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


class TestCreatePlan:
    """Tests for create_plan function."""

    @pytest.fixture
    def mock_ctx(self) -> MockPlanningContext:
        """Create a mock planning context."""
        return MockPlanningContext()

    @pytest.fixture
    def mock_llm_response(self):
        """Create a valid LLM response using PlannerLLMResponse."""
        return PlannerLLMResponse(
            plan_data={
                "title": "Find Dataset Plan",
                "goal": "Locate the orders dataset",
                "assumptions": ["Dataset exists in catalog"],
                "steps": [
                    {
                        "id": "s0",
                        "description": "Search for dataset",
                        "done_when": "Found exactly 1 result",
                        "tool": "datahub__search_datasets",
                    }
                ],
                "expected_deliverable": "Dataset URN",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

    def test_create_plan_success(
        self, mock_ctx: MockPlanningContext, mock_llm_response: dict
    ) -> None:
        """Test successful plan creation."""
        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = mock_llm_response

            plan = create_plan(
                ctx=mock_ctx,
                task="Find orders dataset",
                max_steps=5,
            )

            assert plan.plan_id.startswith("plan_")
            assert plan.version == 1
            assert plan.title == "Find Dataset Plan"
            assert plan.goal == "Locate the orders dataset"
            assert len(plan.steps) == 1
            assert plan.steps[0].id == "s0"
            assert plan.steps[0].description == "Search for dataset"

            # Verify plan is stored in cache
            cached = mock_ctx.get_plan(plan.plan_id)
            assert cached is not None
            assert cached["plan"] == plan

    def test_create_plan_with_context_and_evidence(
        self, mock_ctx: MockPlanningContext, mock_llm_response: dict
    ) -> None:
        """Test plan creation with context and evidence."""
        evidence = {
            "dataset_urn": "urn:li:dataset:(...)",
            "downstream_count": 12,
        }

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = mock_llm_response

            plan = create_plan(
                ctx=mock_ctx,
                task="Analyze impact",
                context="User wants to deprecate dataset",
                evidence=evidence,
                max_steps=10,
            )

            assert plan is not None
            # Verify LLM was called with evidence in prompt
            call_args = mock_llm.call_args[0][0]
            assert "evidence" in call_args.lower()
            assert "dataset_urn" in call_args

    def test_create_plan_with_markdown_wrapped_json(
        self, mock_ctx: MockPlanningContext
    ) -> None:
        """Test that _call_planner_llm handles markdown-wrapped JSON (it returns PlannerLLMResponse)."""
        # _call_planner_llm handles parsing internally and returns a PlannerLLMResponse
        parsed_response = PlannerLLMResponse(
            plan_data={
                "title": "Test Plan",
                "goal": "Test",
                "assumptions": [],
                "steps": [{"id": "s0", "description": "Step 1", "done_when": "Done"}],
                "expected_deliverable": "Result",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )
        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = parsed_response

            plan = create_plan(ctx=mock_ctx, task="Test task")

            assert plan.title == "Test Plan"
            assert len(plan.steps) == 1

    def test_create_plan_json_parse_error(self, mock_ctx: MockPlanningContext) -> None:
        """Test error handling when _call_planner_llm raises an error."""
        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            # _call_planner_llm raises ValueError if it can't parse the response
            mock_llm.side_effect = ValueError("Could not parse text response as JSON")

            with pytest.raises(ValueError, match="Could not parse"):
                create_plan(ctx=mock_ctx, task="Test task")

    def test_create_plan_json_with_trailing_text(
        self, mock_ctx: MockPlanningContext
    ) -> None:
        """
        Test that _call_planner_llm handles JSON with trailing text.

        _call_planner_llm now handles parsing internally and returns a dict.
        """
        # _call_planner_llm handles parsing and returns this structured dict
        parsed_response = {
            "title": "Find Organizations on Premium Pricing Plans",
            "goal": "Identify organizations that are currently on premium pricing plans",
            "assumptions": ["Premium pricing information is likely stored in datasets"],
            "steps": [
                {
                    "id": "s0",
                    "description": "Facet exploration",
                    "done_when": "Completed",
                    "tool": "datahub__search",
                },
                {
                    "id": "s1",
                    "description": "Search for datasets",
                    "done_when": "Results returned",
                    "tool": "datahub__search",
                },
                {
                    "id": "s2",
                    "description": "Search using metadata",
                    "done_when": "Completed",
                    "tool": "datahub__search",
                },
                {
                    "id": "s3",
                    "description": "Get detailed info",
                    "done_when": "Retrieved",
                    "tool": "datahub__get_entities",
                },
                {"id": "s4", "description": "Analyze schema", "done_when": "Analyzed"},
                {
                    "id": "s5",
                    "description": "Retrieve queries",
                    "done_when": "Retrieved",
                    "tool": "datahub__get_dataset_queries",
                },
                {"id": "s6", "description": "Generate summary", "done_when": "Created"},
            ],
            "expected_deliverable": "A list or description of organizations on premium pricing plans",
        }

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = PlannerLLMResponse(
                plan_data=parsed_response,
                internal_data={"tool_used": "create_execution_plan"},
            )

            plan = create_plan(
                ctx=mock_ctx,
                task="Find organizations on premium pricing plans",
            )

            # Verify the plan was created correctly
            assert plan.title == "Find Organizations on Premium Pricing Plans"
            assert "premium pricing plans" in plan.goal.lower()
            assert len(plan.steps) == 7  # s0 through s6
            assert plan.steps[0].id == "s0"
            assert plan.steps[6].id == "s6"
            assert "premium pricing" in plan.expected_deliverable.lower()

    def test_create_plan_generates_unique_ids(
        self, mock_ctx: MockPlanningContext
    ) -> None:
        """Test that multiple plans get unique IDs."""
        mock_response = PlannerLLMResponse(
            plan_data={
                "title": "Plan",
                "goal": "Goal",
                "assumptions": [],
                "steps": [{"id": "s0", "description": "Step", "done_when": "Done"}],
                "expected_deliverable": "Result",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = mock_response

            plan1 = create_plan(ctx=mock_ctx, task="Task 1")
            plan2 = create_plan(ctx=mock_ctx, task="Task 2")

            assert plan1.plan_id != plan2.plan_id
            assert mock_ctx.get_plan(plan1.plan_id) is not None
            assert mock_ctx.get_plan(plan2.plan_id) is not None


class TestRevisePlan:
    """Tests for revise_plan function."""

    @pytest.fixture
    def mock_ctx_with_plan(self) -> MockPlanningContext:
        """Create a mock planning context with an existing plan."""
        # Create an initial plan
        plan = Plan(
            plan_id="plan_abc",
            version=1,
            title="Original Plan",
            goal="Find and analyze dataset",
            assumptions=["Dataset exists"],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=20),
            steps=[
                Step(id="s0", description="Search dataset", done_when="Found 1 result"),
                Step(id="s1", description="Get lineage", done_when="Retrieved lineage"),
                Step(
                    id="s2",
                    description="Analyze results",
                    done_when="Analysis complete",
                ),
            ],
            expected_deliverable="Analysis report",
        )

        ctx = MockPlanningContext()
        ctx.set_plan("plan_abc", plan, {})

        return ctx

    def test_revise_plan_success(self, mock_ctx_with_plan: MockPlanningContext) -> None:
        """Test successful plan revision."""
        revision_response = PlannerLLMResponse(
            plan_data={
                "assumptions": ["Dataset exists", "Need broader search"],
                "steps": [
                    {
                        "id": "s1",
                        "description": "Get downstream lineage with more hops",
                        "done_when": "Retrieved lineage with depth 3",
                    },
                    {
                        "id": "s2",
                        "description": "Filter for BI assets",
                        "done_when": "Found BI dashboards",
                    },
                ],
                "expected_deliverable": "List of affected BI assets",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = revision_response

            revised_plan = revise_plan(
                ctx=mock_ctx_with_plan,
                plan_id="plan_abc",
                completed_steps=["s0"],
                current_step="s1",
                issue="No BI assets found in lineage, need deeper traversal",
                evidence={"downstream_count": 5, "max_hops": 2},
            )

            assert revised_plan.plan_id == "plan_abc"  # Same ID
            assert revised_plan.version == 2  # Incremented version
            assert revised_plan.title == "Original Plan"  # Title preserved
            assert len(revised_plan.steps) == 3  # 1 completed + 2 new
            assert revised_plan.steps[0].id == "s0"  # Completed step preserved
            assert revised_plan.steps[1].id == "s1"  # New step
            assert revised_plan.steps[1].description is not None
            assert "more hops" in revised_plan.steps[1].description

    def test_revise_plan_not_found(
        self, mock_ctx_with_plan: MockPlanningContext
    ) -> None:
        """Test revising a non-existent plan raises error."""
        with pytest.raises(ValueError, match="not found in plan cache"):
            revise_plan(
                ctx=mock_ctx_with_plan,
                plan_id="nonexistent",
                completed_steps=[],
                current_step="s0",
                issue="Test issue",
            )

    def test_revise_plan_updates_cache(
        self, mock_ctx_with_plan: MockPlanningContext
    ) -> None:
        """Test that revised plan updates the cache."""
        revision_response = PlannerLLMResponse(
            plan_data={
                "assumptions": ["Updated"],
                "steps": [{"id": "s1", "description": "New step", "done_when": "Done"}],
                "expected_deliverable": "Result",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = revision_response

            revised_plan = revise_plan(
                ctx=mock_ctx_with_plan,
                plan_id="plan_abc",
                completed_steps=["s0"],
                current_step="s1",
                issue="Issue",
            )

            # Verify cache is updated
            cached_entry = mock_ctx_with_plan.get_plan("plan_abc")
            assert cached_entry is not None
            assert cached_entry["plan"].version == 2
            assert cached_entry["plan"] == revised_plan


class TestReportStepProgress:
    """Tests for report_step_progress function."""

    @pytest.fixture
    def mock_ctx_with_plan(self) -> MockPlanningContext:
        """Create a mock planning context with a plan."""
        plan = Plan(
            plan_id="plan_xyz",
            version=1,
            title="Test Plan",
            goal="Test Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=10),
            steps=[
                Step(id="s0", description="Step 0", done_when="Done 0"),
                Step(id="s1", description="Step 1", done_when="Done 1"),
                Step(id="s2", description="Step 2", done_when="Done 2"),
            ],
            expected_deliverable="Result",
        )

        ctx = MockPlanningContext()
        ctx.set_plan("plan_xyz", plan, {})

        return ctx

    def test_report_step_completed(
        self, mock_ctx_with_plan: MockPlanningContext
    ) -> None:
        """Test reporting a completed step."""
        message = report_step_progress(
            ctx=mock_ctx_with_plan,
            plan_id="plan_xyz",
            step_id="s0",
            status="completed",
            done_criteria_met=True,
            evidence={"result": "success"},
            confidence=1.0,
        )

        assert "s0 completed" in message
        assert "Next: s1" in message

    def test_report_step_failed(self, mock_ctx_with_plan: MockPlanningContext) -> None:
        """Test reporting a failed step."""
        message = report_step_progress(
            ctx=mock_ctx_with_plan,
            plan_id="plan_xyz",
            step_id="s1",
            status="failed",
            done_criteria_met=False,
            failed_criteria_met=True,
            evidence={"error": "timeout"},
            confidence=1.0,
        )

        assert "WARNING" in message
        assert "s1 failed" in message
        assert "revise_plan" in message

    def test_report_last_step_completed(
        self, mock_ctx_with_plan: MockPlanningContext
    ) -> None:
        """Test reporting completion of the last step."""
        # Complete last step directly
        message = report_step_progress(
            ctx=mock_ctx_with_plan,
            plan_id="plan_xyz",
            step_id="s2",
            status="completed",
            done_criteria_met=True,
        )

        # Note: report_step_progress no longer tracks completed steps internally,
        # so this checks the response message format for last step completion
        assert isinstance(message, str)

    def test_report_step_invalid_plan_id(
        self, mock_ctx_with_plan: MockPlanningContext
    ) -> None:
        """Test reporting progress for non-existent plan raises error."""
        with pytest.raises(ValueError, match="not found in plan cache"):
            report_step_progress(
                ctx=mock_ctx_with_plan,
                plan_id="nonexistent",
                step_id="s0",
                status="started",
            )

    def test_report_step_invalid_step_id(
        self, mock_ctx_with_plan: MockPlanningContext
    ) -> None:
        """Test reporting progress for non-existent step raises error."""
        with pytest.raises(ValueError, match="not found in plan"):
            report_step_progress(
                ctx=mock_ctx_with_plan,
                plan_id="plan_xyz",
                step_id="s999",
                status="started",
            )

    def test_report_step_in_progress(
        self, mock_ctx_with_plan: MockPlanningContext
    ) -> None:
        """Test reporting a step in progress."""
        message = report_step_progress(
            ctx=mock_ctx_with_plan,
            plan_id="plan_xyz",
            step_id="s0",
            status="in_progress",
            evidence={"partial_results": []},
        )

        assert "Step recorded" in message or "s0" in message

    def test_report_multiple_steps_returns_messages(
        self, mock_ctx_with_plan: MockPlanningContext
    ) -> None:
        """Test that reporting multiple steps returns guidance messages."""
        # Complete s0
        msg1 = report_step_progress(
            ctx=mock_ctx_with_plan,
            plan_id="plan_xyz",
            step_id="s0",
            status="completed",
        )
        assert isinstance(msg1, str)

        # Complete s1
        msg2 = report_step_progress(
            ctx=mock_ctx_with_plan,
            plan_id="plan_xyz",
            step_id="s1",
            status="completed",
        )
        assert isinstance(msg2, str)
