"""
Unit tests for chat planner tools.

Tests the planning tool functions including create_plan, revise_plan,
and report_step_progress.
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.chat.planner.models import Constraints, OnFail, Plan, Step
from datahub_integrations.chat.planner.tools import (
    _get_available_tools,
    _get_plan_tool_spec,
    _get_tool_descriptions,
    create_plan,
    get_plan_by_id,
    report_step_progress,
    revise_plan,
)


class TestGetPlanById:
    """Tests for get_plan_by_id function."""

    def test_get_plan_by_id_found(self):
        """Test retrieving an existing plan."""
        # Create mock session with plan cache
        mock_session = MagicMock()
        test_plan = Plan(
            plan_id="plan_test",
            version=1,
            title="Test Plan",
            goal="Test goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[],
            expected_deliverable="Test deliverable",
        )
        mock_session.plan_cache = {"plan_test": {"plan": test_plan}}

        # Execute
        result = get_plan_by_id("plan_test", mock_session)

        # Verify
        assert result == test_plan
        assert result.plan_id == "plan_test"

    def test_get_plan_by_id_not_found(self):
        """Test retrieving a non-existent plan."""
        # Create mock session with empty cache
        mock_session = MagicMock()
        mock_session.plan_cache = {}

        # Execute
        result = get_plan_by_id("nonexistent", mock_session)

        # Verify
        assert result is None


class TestGetAvailableTools:
    """Tests for _get_available_tools function."""

    def test_get_available_tools(self):
        """Test getting list of available tool names."""
        # Create mock session with tools
        mock_session = MagicMock()
        mock_tool1 = MagicMock()
        mock_tool1.name = "datahub__search"
        mock_tool2 = MagicMock()
        mock_tool2.name = "datahub__get_entity"
        mock_session.get_plannable_tools.return_value = [mock_tool1, mock_tool2]

        # Execute
        result = _get_available_tools(mock_session)

        # Verify
        assert result == ["datahub__search", "datahub__get_entity"]


class TestGetToolDescriptions:
    """Tests for _get_tool_descriptions function."""

    def test_get_tool_descriptions(self):
        """Test getting tool names and descriptions."""
        # Create mock session with tools
        mock_session = MagicMock()

        mock_tool1 = MagicMock()
        mock_tool1.name = "datahub__search"
        mock_tool1._tool.description = "Search for entities"

        mock_tool2 = MagicMock()
        mock_tool2.name = "datahub__get_entity"
        mock_tool2._tool.description = "Get entity details"

        mock_session.get_plannable_tools.return_value = [mock_tool1, mock_tool2]

        # Execute
        result = _get_tool_descriptions(mock_session)

        # Verify
        assert result == {
            "datahub__search": "Search for entities",
            "datahub__get_entity": "Get entity details",
        }

    def test_get_tool_descriptions_handles_missing_description(self):
        """Test that missing descriptions are handled."""
        # Create mock session with tool missing description
        mock_session = MagicMock()
        mock_tool = MagicMock()
        mock_tool.name = "datahub__search"
        mock_tool._tool.description = None
        mock_session.get_plannable_tools.return_value = [mock_tool]

        # Execute
        result = _get_tool_descriptions(mock_session)

        # Verify
        assert result == {"datahub__search": ""}


class TestGetPlanToolSpec:
    """Tests for _get_plan_tool_spec function."""

    def test_get_plan_tool_spec_structure(self):
        """Test that plan tool spec has correct structure."""
        spec = _get_plan_tool_spec()

        # Verify top-level structure
        assert "toolSpec" in spec
        assert "name" in spec["toolSpec"]
        assert "description" in spec["toolSpec"]
        assert "inputSchema" in spec["toolSpec"]

        # Verify tool name
        assert spec["toolSpec"]["name"] == "create_execution_plan"

        # Verify schema
        schema = spec["toolSpec"]["inputSchema"]["json"]
        assert schema["type"] == "object"
        assert "properties" in schema

    def test_get_plan_tool_spec_excludes_auto_fields(self):
        """Test that auto-generated fields are excluded from schema."""
        spec = _get_plan_tool_spec()
        schema = spec["toolSpec"]["inputSchema"]["json"]
        properties = schema.get("properties", {})

        # These fields should be excluded (set by code, not LLM)
        assert "plan_id" not in properties
        assert "version" not in properties
        assert "metadata" not in properties

    def test_get_plan_tool_spec_excludes_auto_from_required(self):
        """Test that auto fields are not in required list."""
        spec = _get_plan_tool_spec()
        schema = spec["toolSpec"]["inputSchema"]["json"]
        required = schema.get("required", [])

        # These should not be required
        assert "plan_id" not in required
        assert "version" not in required
        assert "metadata" not in required


class TestCreatePlan:
    """Tests for create_plan function."""

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools._get_tool_descriptions")
    @patch("datahub_integrations.chat.planner.tools._get_available_tools")
    def test_create_plan_basic(
        self, mock_get_tools, mock_get_descriptions, mock_call_llm
    ):
        """Test basic plan creation."""
        # Setup mocks
        mock_get_tools.return_value = ["datahub__search", "datahub__get_entity"]
        mock_get_descriptions.return_value = {
            "datahub__search": "Search entities",
            "datahub__get_entity": "Get entity",
        }

        mock_call_llm.return_value = {
            "title": "Test Plan",
            "goal": "Find datasets",
            "assumptions": ["DataHub is accessible"],
            "steps": [
                {
                    "id": "s0",
                    "description": "Search for datasets",
                    "done_when": "Found results",
                    "on_fail": {"action": "retry"},
                }
            ],
            "expected_deliverable": "List of datasets",
        }

        # Create mock session
        mock_session = MagicMock()
        mock_session.plan_cache = {}

        # Execute
        plan = create_plan(
            session=mock_session,
            task="Find all datasets with PII",
            context=None,
            evidence=None,
            max_steps=10,
        )

        # Verify
        assert isinstance(plan, Plan)
        assert plan.plan_id.startswith("plan_")
        assert plan.version == 1
        assert plan.title == "Test Plan"
        assert plan.goal == "Find datasets"
        assert len(plan.steps) == 1
        assert plan.steps[0].id == "s0"

        # Verify plan was stored in cache
        assert plan.plan_id in mock_session.plan_cache
        cached_entry = mock_session.plan_cache[plan.plan_id]
        assert cached_entry["plan"] == plan
        assert "progress" in cached_entry

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools._get_tool_descriptions")
    @patch("datahub_integrations.chat.planner.tools._get_available_tools")
    def test_create_plan_with_context_and_evidence(
        self, mock_get_tools, mock_get_descriptions, mock_call_llm
    ):
        """Test plan creation with context and evidence."""
        mock_get_tools.return_value = ["datahub__search"]
        mock_get_descriptions.return_value = {"datahub__search": "Search"}

        mock_call_llm.return_value = {
            "title": "Test Plan",
            "goal": "Test",
            "assumptions": [],
            "steps": [
                {
                    "id": "s0",
                    "description": "Test step",
                    "done_when": "Done",
                    "on_fail": {"action": "abort"},
                }
            ],
            "expected_deliverable": "Result",
        }

        mock_session = MagicMock()
        mock_session.plan_cache = {}

        # Execute with context and evidence
        context = "We previously found 10 candidates"
        evidence = {"dataset_urn": "urn:li:dataset:test", "count": 10}

        create_plan(
            session=mock_session,
            task="Analyze dataset",
            context=context,
            evidence=evidence,
            max_steps=5,
        )

        # Verify LLM was called with context and evidence
        mock_call_llm.assert_called_once()
        call_args = mock_call_llm.call_args[0][0]  # First positional arg (prompt)
        assert context in call_args
        assert "urn:li:dataset:test" in call_args
        assert "10" in call_args

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools._get_tool_descriptions")
    @patch("datahub_integrations.chat.planner.tools._get_available_tools")
    def test_create_plan_sets_constraints(
        self, mock_get_tools, mock_get_descriptions, mock_call_llm
    ):
        """Test that constraints are set correctly."""
        mock_get_tools.return_value = ["tool1", "tool2"]
        mock_get_descriptions.return_value = {"tool1": "T1", "tool2": "T2"}

        mock_call_llm.return_value = {
            "title": "Test",
            "goal": "Test",
            "assumptions": [],
            "steps": [],
            "expected_deliverable": "Result",
            "max_tool_calls": 25,
        }

        mock_session = MagicMock()
        mock_session.plan_cache = {}

        # Execute
        plan = create_plan(mock_session, "Test task")

        # Verify constraints
        assert plan.constraints.tool_allowlist == ["tool1", "tool2"]
        assert plan.constraints.max_tool_calls == 25


class TestRevisePlan:
    """Tests for revise_plan function."""

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools._get_tool_descriptions")
    def test_revise_plan_basic(self, mock_get_descriptions, mock_call_llm):
        """Test basic plan revision."""
        # Setup mocks
        mock_get_descriptions.return_value = {"datahub__search": "Search"}

        mock_call_llm.return_value = {
            "assumptions": ["Updated assumption"],
            "steps": [
                {
                    "id": "s1",
                    "description": "New approach",
                    "done_when": "Completed",
                    "on_fail": {"action": "retry"},
                }
            ],
            "expected_deliverable": "Result",
        }

        # Create original plan
        original_plan = Plan(
            plan_id="plan_test",
            version=1,
            title="Original Plan",
            goal="Find data",
            assumptions=["Original assumption"],
            constraints=Constraints(
                tool_allowlist=["datahub__search"], max_tool_calls=10
            ),
            steps=[
                Step(
                    id="s0",
                    description="First step",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                )
            ],
            expected_deliverable="Original result",
        )

        # Create mock session with cached plan
        mock_session = MagicMock()
        mock_session.plan_cache = {"plan_test": {"plan": original_plan, "progress": {}}}

        # Execute
        revised_plan = revise_plan(
            session=mock_session,
            plan_id="plan_test",
            completed_steps=["s0"],
            current_step="s1",
            issue="No results found",
            evidence={"search_count": 0},
        )

        # Verify
        assert revised_plan.plan_id == "plan_test"  # Same ID
        assert revised_plan.version == 2  # Incremented
        assert revised_plan.title == "Original Plan"  # Preserved
        assert len(revised_plan.steps) == 2  # s0 (completed) + s1 (new)
        assert revised_plan.steps[0].id == "s0"  # Original s0 preserved
        assert revised_plan.steps[1].id == "s1"  # New s1

        # Verify cache was updated
        assert mock_session.plan_cache["plan_test"]["plan"] == revised_plan

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools._get_tool_descriptions")
    def test_revise_plan_not_found(self, mock_get_descriptions, mock_call_llm):
        """Test revising a non-existent plan raises error."""
        mock_session = MagicMock()
        mock_session.plan_cache = {}

        # Execute and expect error
        with pytest.raises(ValueError, match="Plan .* not found"):
            revise_plan(
                session=mock_session,
                plan_id="nonexistent",
                completed_steps=[],
                current_step="s0",
                issue="test",
            )

    @patch("datahub_integrations.chat.planner.tools._call_planner_llm")
    @patch("datahub_integrations.chat.planner.tools._get_tool_descriptions")
    def test_revise_plan_preserves_completed_steps(
        self, mock_get_descriptions, mock_call_llm
    ):
        """Test that completed steps are preserved."""
        mock_get_descriptions.return_value = {}
        mock_call_llm.return_value = {
            "assumptions": [],
            "steps": [
                {
                    "id": "s2",
                    "description": "New step 2",
                    "done_when": "Done",
                    "on_fail": {"action": "retry"},
                }
            ],
            "expected_deliverable": "Result",
        }

        # Create plan with multiple steps
        original_plan = Plan(
            plan_id="plan_test",
            version=1,
            title="Test",
            goal="Test",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(
                    id="s0",
                    description="Step 0",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                ),
                Step(
                    id="s1",
                    description="Step 1",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                ),
                Step(
                    id="s2",
                    description="Step 2",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                ),
            ],
            expected_deliverable="Result",
        )

        mock_session = MagicMock()
        mock_session.plan_cache = {"plan_test": {"plan": original_plan, "progress": {}}}

        # Revise from s2 onward, s0 and s1 completed
        revised_plan = revise_plan(
            session=mock_session,
            plan_id="plan_test",
            completed_steps=["s0", "s1"],
            current_step="s2",
            issue="Need different approach",
        )

        # Verify completed steps preserved
        assert len(revised_plan.steps) == 3  # s0, s1 (preserved), s2 (new)
        assert revised_plan.steps[0].id == "s0"
        assert revised_plan.steps[0].description == "Step 0"
        assert revised_plan.steps[1].id == "s1"
        assert revised_plan.steps[1].description == "Step 1"
        assert revised_plan.steps[2].id == "s2"
        assert revised_plan.steps[2].description == "New step 2"


class TestReportStepProgress:
    """Tests for report_step_progress function."""

    def test_report_step_progress_completed(self):
        """Test reporting a completed step."""
        # Create plan
        plan = Plan(
            plan_id="plan_test",
            version=1,
            title="Test",
            goal="Test",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(
                    id="s0",
                    description="Step 0",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                ),
                Step(
                    id="s1",
                    description="Step 1",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                ),
            ],
            expected_deliverable="Result",
        )

        # Create mock session
        mock_session = MagicMock()
        mock_session.plan_cache = {"plan_test": {"plan": plan, "progress": {}}}

        # Report s0 completed
        message = report_step_progress(
            session=mock_session,
            plan_id="plan_test",
            step_id="s0",
            status="completed",
            done_criteria_met=True,
            evidence={"urn": "urn:li:dataset:test"},
            confidence=1.0,
        )

        # Verify message
        assert "Step s0 completed" in message
        assert "Next: s1" in message
        assert "1/2 complete" in message

        # Verify progress was stored
        progress = mock_session.plan_cache["plan_test"]["progress"]
        assert "s0" in progress
        assert progress["s0"]["status"] == "completed"
        assert progress["s0"]["evidence"] == {"urn": "urn:li:dataset:test"}
        assert progress["s0"]["confidence"] == 1.0
        assert "timestamp" in progress["s0"]

    def test_report_step_progress_failed(self):
        """Test reporting a failed step."""
        plan = Plan(
            plan_id="plan_test",
            version=1,
            title="Test",
            goal="Test",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(
                    id="s0",
                    description="Step 0",
                    done_when="Done",
                    on_fail=OnFail(action="retry"),
                )
            ],
            expected_deliverable="Result",
        )

        mock_session = MagicMock()
        mock_session.plan_cache = {"plan_test": {"plan": plan, "progress": {}}}

        # Report s0 failed
        message = report_step_progress(
            session=mock_session,
            plan_id="plan_test",
            step_id="s0",
            status="failed",
            done_criteria_met=False,
            failed_criteria_met=True,
            evidence={"error": "Timeout"},
        )

        # Verify message
        assert "WARNING" in message
        assert "failed" in message
        assert "revise_plan" in message

    def test_report_step_progress_all_complete(self):
        """Test reporting when all steps are complete."""
        plan = Plan(
            plan_id="plan_test",
            version=1,
            title="Test",
            goal="Test",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(
                    id="s0",
                    description="Step 0",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                )
            ],
            expected_deliverable="Result",
        )

        mock_session = MagicMock()
        mock_session.plan_cache = {
            "plan_test": {"plan": plan, "progress": {"s0": {"status": "completed"}}}
        }

        # Report s0 completed (again)
        message = report_step_progress(
            session=mock_session,
            plan_id="plan_test",
            step_id="s0",
            status="completed",
        )

        # Verify message
        assert "Plan complete" in message
        assert "1/1" in message or "All 1" in message

    def test_report_step_progress_plan_not_found(self):
        """Test reporting progress for non-existent plan."""
        mock_session = MagicMock()
        mock_session.plan_cache = {}

        # Execute and expect error
        with pytest.raises(ValueError, match="Plan .* not found"):
            report_step_progress(
                session=mock_session,
                plan_id="nonexistent",
                step_id="s0",
                status="completed",
            )

    def test_report_step_progress_step_not_found(self):
        """Test reporting progress for non-existent step."""
        plan = Plan(
            plan_id="plan_test",
            version=1,
            title="Test",
            goal="Test",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(
                    id="s0",
                    description="Step 0",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                )
            ],
            expected_deliverable="Result",
        )

        mock_session = MagicMock()
        mock_session.plan_cache = {"plan_test": {"plan": plan, "progress": {}}}

        # Execute and expect error
        with pytest.raises(ValueError, match="Step .* not found"):
            report_step_progress(
                session=mock_session,
                plan_id="plan_test",
                step_id="s999",  # Non-existent
                status="completed",
            )

    def test_report_step_progress_updates_existing(self):
        """Test that reporting progress updates existing progress."""
        plan = Plan(
            plan_id="plan_test",
            version=1,
            title="Test",
            goal="Test",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(
                    id="s0",
                    description="Step 0",
                    done_when="Done",
                    on_fail=OnFail(action="abort"),
                )
            ],
            expected_deliverable="Result",
        )

        mock_session = MagicMock()
        mock_session.plan_cache = {
            "plan_test": {
                "plan": plan,
                "progress": {"s0": {"status": "in_progress", "evidence": {}}},
            }
        }

        # Update s0 to completed
        report_step_progress(
            session=mock_session,
            plan_id="plan_test",
            step_id="s0",
            status="completed",
            evidence={"result": "success"},
        )

        # Verify progress was updated
        progress = mock_session.plan_cache["plan_test"]["progress"]["s0"]
        assert progress["status"] == "completed"
        assert progress["evidence"] == {"result": "success"}
