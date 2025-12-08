"""Unit tests for planner tools."""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.agent import AgentRunner
from datahub_integrations.chat.planner.models import Constraints, Plan, Step
from datahub_integrations.chat.planner.tools import (
    PlannerLLMResponse,
    _call_planner_llm,
    _get_available_tools,
    _get_planner_tool_specs,
    _get_tool_descriptions,
    create_plan,
    get_plan_by_id,
    get_planning_tool_wrappers,
    report_step_progress,
    revise_plan,
)
from datahub_integrations.mcp_integration.tool import ToolWrapper


class TestGetPlanById:
    """Test get_plan_by_id function."""

    def test_returns_plan_when_exists(self) -> None:
        """Test retrieving existing plan from cache."""
        mock_agent = Mock(spec=AgentRunner)
        test_plan = Mock(spec=Plan)
        test_plan.plan_id = "plan_123"

        mock_agent.plan_cache = {"plan_123": {"plan": test_plan, "progress": {}}}

        result = get_plan_by_id("plan_123", mock_agent)

        assert result == test_plan

    def test_returns_none_when_not_exists(self) -> None:
        """Test returns None for non-existent plan."""
        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {}

        result = get_plan_by_id("nonexistent", mock_agent)

        assert result is None

    def test_returns_none_when_cache_malformed(self) -> None:
        """Test handles malformed cache gracefully."""
        mock_agent = Mock(spec=AgentRunner)
        # Cache entry without 'plan' key
        mock_agent.plan_cache = {"plan_123": {"progress": {}}}

        result = get_plan_by_id("plan_123", mock_agent)

        assert result is None


class TestReportStepProgress:
    """Test report_step_progress function."""

    def test_reports_completed_step(self) -> None:
        """Test reporting a completed step."""
        mock_agent = Mock(spec=AgentRunner)

        # Create a simple plan with two steps
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test Plan",
            goal="Test goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(id="s0", description="First step", done_when="Done 0"),
                Step(id="s1", description="Second step", done_when="Done 1"),
            ],
            expected_deliverable="Test result",
        )
        mock_agent.plan_cache = {"plan_123": {"plan": test_plan, "progress": {}}}

        result = report_step_progress(
            agent=mock_agent,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            done_criteria_met=True,
            evidence={"found": 1},
            confidence=1.0,
        )

        # Verify progress was recorded
        assert "s0" in mock_agent.plan_cache["plan_123"]["progress"]
        assert (
            mock_agent.plan_cache["plan_123"]["progress"]["s0"]["status"] == "completed"
        )

        # Verify response mentions progress
        assert isinstance(result, str)
        assert "s0" in result
        assert "completed" in result.lower() or "Next" in result

    def test_reports_failed_step(self) -> None:
        """Test reporting a failed step."""
        mock_agent = Mock(spec=AgentRunner)
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[Step(id="s0", description="First step", done_when="Done")],
            expected_deliverable="Result",
        )
        mock_agent.plan_cache = {"plan_123": {"plan": test_plan, "progress": {}}}

        result = report_step_progress(
            agent=mock_agent,
            plan_id="plan_123",
            step_id="s0",
            status="failed",
            done_criteria_met=False,
            failed_criteria_met=True,
        )

        # Verify failure recorded
        assert mock_agent.plan_cache["plan_123"]["progress"]["s0"]["status"] == "failed"

        # Verify response mentions failure
        assert "failed" in result.lower() or "WARNING" in result

    def test_raises_error_for_nonexistent_plan(self) -> None:
        """Test raises error when plan doesn't exist."""
        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {}

        with pytest.raises(ValueError, match="not found"):
            report_step_progress(
                agent=mock_agent,
                plan_id="nonexistent",
                step_id="s0",
                status="completed",
            )

    def test_raises_error_for_invalid_step_id(self) -> None:
        """Test raises error when step_id doesn't exist in plan."""
        mock_agent = Mock(spec=AgentRunner)
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[Step(id="s0", description="Step 0", done_when="Done")],
            expected_deliverable="Result",
        )
        mock_agent.plan_cache = {"plan_123": {"plan": test_plan, "progress": {}}}

        with pytest.raises(ValueError, match="not found"):
            report_step_progress(
                agent=mock_agent,
                plan_id="plan_123",
                step_id="s99",  # Invalid step ID
                status="completed",
            )

    def test_stores_evidence(self) -> None:
        """Test that evidence is stored in progress."""
        mock_agent = Mock(spec=AgentRunner)
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )
        mock_agent.plan_cache = {"plan_123": {"plan": test_plan, "progress": {}}}

        evidence_data = {"urn": "test_urn", "count": 5}

        report_step_progress(
            agent=mock_agent,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            evidence=evidence_data,
        )

        stored_evidence = mock_agent.plan_cache["plan_123"]["progress"]["s0"][
            "evidence"
        ]
        assert stored_evidence == evidence_data

    def test_stores_confidence(self) -> None:
        """Test that confidence is stored."""
        mock_agent = Mock(spec=AgentRunner)
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )
        mock_agent.plan_cache = {"plan_123": {"plan": test_plan, "progress": {}}}

        report_step_progress(
            agent=mock_agent,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            confidence=0.85,
        )

        stored_confidence = mock_agent.plan_cache["plan_123"]["progress"]["s0"][
            "confidence"
        ]
        assert stored_confidence == 0.85

    def test_reports_plan_completion(self) -> None:
        """Test reporting when all steps are complete."""
        mock_agent = Mock(spec=AgentRunner)
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(id="s0", description="Step 0", done_when="Done 0"),
                Step(id="s1", description="Step 1", done_when="Done 1"),
            ],
            expected_deliverable="Result",
        )
        mock_agent.plan_cache = {
            "plan_123": {
                "plan": test_plan,
                "progress": {
                    "s0": {
                        "status": "completed",
                        "evidence": {},
                        "confidence": None,
                        "timestamp": "2024-01-01",
                    }
                },
            }
        }

        # Complete the last step
        result = report_step_progress(
            agent=mock_agent,
            plan_id="plan_123",
            step_id="s1",
            status="completed",
        )

        # Should indicate plan completion
        assert "complete" in result.lower()
        assert "2/2" in result or "All" in result

    def test_updates_existing_step_progress(self) -> None:
        """Test that reporting same step again updates the progress."""
        mock_agent = Mock(spec=AgentRunner)
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )
        mock_agent.plan_cache = {
            "plan_123": {
                "plan": test_plan,
                "progress": {
                    "s0": {
                        "status": "started",
                        "evidence": {},
                        "confidence": None,
                        "timestamp": "old_time",
                    }
                },
            }
        }

        # Update to completed
        report_step_progress(
            agent=mock_agent,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
        )

        # Should have updated status
        assert (
            mock_agent.plan_cache["plan_123"]["progress"]["s0"]["status"] == "completed"
        )


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
                "max_tool_calls": 10,
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        mock_get_instructions.return_value = None

        # Create mock agent
        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {}
        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool._tool = Mock()
        mock_tool._tool.description = "Test tool description"
        mock_agent.get_plannable_tools.return_value = [mock_tool]

        # Create plan
        result = create_plan(agent=mock_agent, task="Test task")

        # Verify plan structure
        assert isinstance(result, Plan)
        assert result.title == "Test Plan"
        assert result.goal == "Test goal"
        assert len(result.steps) == 1
        assert result.steps[0].id == "s0"
        assert result.version == 1

        # Verify plan was cached
        assert result.plan_id in mock_agent.plan_cache

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

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {}
        mock_agent.get_plannable_tools.return_value = []

        result = create_plan(
            agent=mock_agent,
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

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {}
        mock_agent.get_plannable_tools.return_value = []

        create_plan(agent=mock_agent, task="Task", max_steps=5)

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
                tool_allowlist=["tool1", "tool2"], max_tool_calls=20
            ),
            steps=[
                Step(id="s0", description="Step 0", done_when="Done 0"),
                Step(id="s1", description="Step 1", done_when="Done 1"),
                Step(id="s2", description="Step 2", done_when="Done 2"),
            ],
            expected_deliverable="Original deliverable",
        )

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {"plan_123": {"plan": original_plan, "progress": {}}}
        mock_agent.get_plannable_tools.return_value = []

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
            agent=mock_agent,
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
        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {}

        with pytest.raises(ValueError, match="not found"):
            revise_plan(
                agent=mock_agent,
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
            constraints=Constraints(tool_allowlist=["tool1"], max_tool_calls=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Deliverable",
        )

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {"plan_123": {"plan": original_plan, "progress": {}}}
        mock_agent.get_plannable_tools.return_value = []

        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "assumptions": [],
                "steps": [{"id": "s0", "description": "Revised", "done_when": "Done"}],
                "expected_deliverable": "New deliverable",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        revised = revise_plan(
            agent=mock_agent,
            plan_id="plan_123",
            completed_steps=[],
            current_step="s0",
            issue="Issue",
        )

        # Verify cache was updated
        assert mock_agent.plan_cache["plan_123"]["plan"] == revised
        assert mock_agent.plan_cache["plan_123"]["plan"].version == 2


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

    def test_get_available_tools(self) -> None:
        """Test getting available tool names."""

        mock_tool1 = Mock(spec=ToolWrapper)
        mock_tool1.name = "datahub__search"
        mock_tool2 = Mock(spec=ToolWrapper)
        mock_tool2.name = "datahub__get_entities"

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.get_plannable_tools.return_value = [mock_tool1, mock_tool2]

        result = _get_available_tools(mock_agent)

        assert result == ["datahub__search", "datahub__get_entities"]

    def test_get_tool_descriptions(self) -> None:
        """Test getting tool descriptions."""

        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool._tool = Mock()
        mock_tool._tool.description = "Test description"

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.get_plannable_tools.return_value = [mock_tool]

        result = _get_tool_descriptions(mock_agent)

        assert result == {"test_tool": "Test description"}

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

        mock_agent = Mock(spec=AgentRunner)

        result = get_planning_tool_wrappers(mock_agent)

        assert len(result) == 2
        tool_names = [tool.name for tool in result]
        assert "create_plan" in tool_names
        assert "revise_plan" in tool_names
        # report_step_progress intentionally not registered (latency optimization)

    def test_tool_wrappers_have_descriptions(self) -> None:
        """Test that all planning tools have descriptions."""

        mock_agent = Mock(spec=AgentRunner)

        tools = get_planning_tool_wrappers(mock_agent)

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

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {}
        mock_agent.get_plannable_tools.return_value = []

        plan = create_plan(agent=mock_agent, task="Task")

        # Verify cache entry
        assert plan.plan_id in mock_agent.plan_cache
        cache_entry = mock_agent.plan_cache[plan.plan_id]
        assert "plan" in cache_entry
        assert "progress" in cache_entry
        assert "internal" in cache_entry
        assert cache_entry["plan"] == plan
        assert isinstance(cache_entry["progress"], dict)
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
            constraints=Constraints(tool_allowlist=["tool1"], max_tool_calls=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {"plan_123": {"plan": original_plan, "progress": {}}}
        mock_agent.get_plannable_tools.return_value = []

        mock_call_llm.return_value = PlannerLLMResponse(
            plan_data={
                "assumptions": [],
                "steps": [{"id": "s0", "description": "Revised", "done_when": "Done"}],
                "expected_deliverable": "Result",
            },
            internal_data={"tool_used": "create_execution_plan"},
        )

        revised = revise_plan(
            agent=mock_agent,
            plan_id="plan_123",
            completed_steps=[],
            current_step="s0",
            issue="Issue",
        )

        # Verify same cache key is used
        assert "plan_123" in mock_agent.plan_cache
        assert mock_agent.plan_cache["plan_123"]["plan"] == revised
        # Progress should be preserved
        assert "progress" in mock_agent.plan_cache["plan_123"]


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

        mock_agent = Mock(spec=AgentRunner)
        mock_agent.plan_cache = {}
        mock_agent.get_plannable_tools.return_value = []

        result = create_plan(agent=mock_agent, task="")

        assert isinstance(result, Plan)

    def test_report_progress_with_no_evidence(self) -> None:
        """Test reporting progress without evidence."""
        mock_agent = Mock(spec=AgentRunner)
        mock_plan = Mock(spec=Plan)
        mock_plan.steps = [
            Mock(id="s0", description="Step 0"),
            Mock(id="s1", description="Step 1"),
        ]
        mock_agent.plan_cache = {"plan_123": {"plan": mock_plan, "progress": {}}}

        result = report_step_progress(
            agent=mock_agent,
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
        mock_agent = Mock(spec=AgentRunner)
        test_plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test",
            goal="Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[Step(id="s0", description="Step", done_when="Done")],
            expected_deliverable="Result",
        )
        mock_agent.plan_cache = {"plan_123": {"plan": test_plan, "progress": {}}}

        result = report_step_progress(
            agent=mock_agent,
            plan_id="plan_123",
            step_id="s0",
            status="completed",
            confidence=None,
        )

        # Should accept None confidence
        assert isinstance(result, str)
        assert mock_agent.plan_cache["plan_123"]["progress"]["s0"]["confidence"] is None
