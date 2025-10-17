"""
Comprehensive unit tests for planning functions in tools.py.

Tests the core logic of create_plan, revise_plan, report_step_progress,
and related helper functions.
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.chat.chat_session import ChatSession
from datahub_integrations.chat.planner.models import Constraints, Plan, Step
from datahub_integrations.chat.planner.tools import (
    create_plan,
    get_plan_by_id,
    report_step_progress,
    revise_plan,
)


class TestGetPlanById:
    """Tests for get_plan_by_id function."""

    @pytest.fixture
    def mock_session(self) -> ChatSession:
        """Create a mock session with a plan cache."""
        session = MagicMock(spec=ChatSession)
        session.session_id = "test_session"
        session.plan_cache = {}
        return session

    def test_get_plan_by_id_found(self, mock_session: ChatSession) -> None:
        """Test retrieving an existing plan."""
        plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test Plan",
            goal="Test Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[],
            expected_deliverable="Result",
        )
        mock_session.plan_cache["plan_123"] = {"plan": plan, "progress": {}}

        result = get_plan_by_id("plan_123", mock_session)

        assert result is not None
        assert result.plan_id == "plan_123"
        assert result.title == "Test Plan"

    def test_get_plan_by_id_not_found(self, mock_session: ChatSession) -> None:
        """Test retrieving a non-existent plan returns None."""
        result = get_plan_by_id("nonexistent", mock_session)
        assert result is None

    def test_get_plan_by_id_empty_cache(self, mock_session: ChatSession) -> None:
        """Test retrieving from empty cache returns None."""
        mock_session.plan_cache = {}
        result = get_plan_by_id("any_id", mock_session)
        assert result is None


class TestCreatePlan:
    """Tests for create_plan function."""

    @pytest.fixture
    def mock_session(self) -> ChatSession:
        """Create a mock session."""
        session = MagicMock(spec=ChatSession)
        session.session_id = "test_session"
        session.plan_cache = {}
        session.get_plannable_tools = MagicMock(return_value=[])
        return session

    @pytest.fixture
    def mock_llm_response(self) -> str:
        """Create a valid LLM response JSON."""
        return """{
            "title": "Find Dataset Plan",
            "goal": "Locate the orders dataset",
            "assumptions": ["Dataset exists in catalog"],
            "steps": [
                {
                    "id": "s0",
                    "description": "Search for dataset",
                    "done_when": "Found exactly 1 result",
                    "tool": "datahub__search_datasets"
                }
            ],
            "expected_deliverable": "Dataset URN"
        }"""

    def test_create_plan_success(
        self, mock_session: ChatSession, mock_llm_response: str
    ) -> None:
        """Test successful plan creation."""
        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = mock_llm_response

            plan = create_plan(
                session=mock_session,
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
            assert plan.plan_id in mock_session.plan_cache
            cached = mock_session.plan_cache[plan.plan_id]
            assert cached["plan"] == plan
            assert cached["progress"] == {}

    def test_create_plan_with_context_and_evidence(
        self, mock_session: ChatSession, mock_llm_response: str
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
                session=mock_session,
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
        self, mock_session: ChatSession
    ) -> None:
        """Test handling of JSON wrapped in markdown code blocks."""
        markdown_response = """Here's the plan:
```json
{
    "title": "Test Plan",
    "goal": "Test",
    "assumptions": [],
    "steps": [{"id": "s0", "description": "Step 1", "done_when": "Done"}],
    "expected_deliverable": "Result"
}
```
"""
        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = markdown_response

            plan = create_plan(session=mock_session, task="Test task")

            assert plan.title == "Test Plan"
            assert len(plan.steps) == 1

    def test_create_plan_json_parse_error(self, mock_session: ChatSession) -> None:
        """Test error handling when LLM returns invalid JSON."""
        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = "This is not JSON"

            with pytest.raises(ValueError, match="Invalid JSON in plan response"):
                create_plan(session=mock_session, task="Test task")

    def test_create_plan_json_with_trailing_text(
        self, mock_session: ChatSession
    ) -> None:
        """
        Test handling of valid JSON followed by extra text (reproduces issue #6840).

        This reproduces the error: "Extra data: line 97 column 1 (char 4202)"
        which occurs when the LLM returns valid JSON followed by explanatory text.
        """
        # Exact JSON from the bug report, followed by trailing text
        llm_response_with_trailing_text = """{ "title": "Find Organizations on Premium Pricing Plans", "goal": "Identify organizations that are currently on premium pricing plans by discovering and analyzing relevant datasets", "assumptions": [ "Premium pricing information is likely stored in datasets related to customers, subscriptions, or pricing", "The organization may have structured metadata (tags, glossary terms) related to premium pricing", "For compliance purposes, only explicitly tagged entities count - no lineage inference", "This plan should focus on discovering the most relevant data sources first" ], "constraints": { "tool_allowlist": ["datahub__search", "datahub__get_entities", "datahub__get_dataset_queries"], "max_tool_calls": 10 }, "steps": [ { "id": "s0", "description": "Facet exploration to discover metadata related to premium pricing or plans", "done_when": "Facet exploration completed and potential metadata (tags, glossary terms, domains) related to premium pricing identified", "tool": "datahub__search", "param_hints": { "query": "", "filters": {"entity_type": ["DATASET"]}, "num_results": 0 }, "on_fail": {"action": "abort"} }, { "id": "s1", "description": "Search for datasets with keywords related to premium pricing plans", "done_when": "Search completed and returned results related to premium pricing plans", "tool": "datahub__search", "param_hints": { "query": "/q premium+pricing OR premium+plan OR pricing+tier OR subscription+tier OR organization+pricing", "filters": {"entity_type": ["DATASET"]}, "num_results": 10 }, "on_fail": {"action": "abort"} }, { "id": "s2", "description": "If metadata found in s0, search for datasets using those specific tags or terms", "done_when": "Search using metadata URNs completed or skipped if no relevant metadata found in s0", "tool": "datahub__search", "param_hints": { "query": "", "filters": { "and": [ {"entity_type": ["DATASET"]}, {"or": [ {"tag": ["<EXTRACT_FROM_S0_premium_tags>"]}, {"glossary_term": ["<EXTRACT_FROM_S0_premium_terms>"]} ]} ] }, "num_results": 10 }, "on_fail": {"action": "continue"} }, { "id": "s3", "description": "Get detailed information about the most promising datasets from searches", "done_when": "Retrieved detailed information for top datasets from previous searches", "tool": "datahub__get_entities", "param_hints": { "urns": ["<TOP_3_URNS_FROM_S1_OR_S2>"] }, "on_fail": {"action": "abort"} }, { "id": "s4", "description": "Analyze schema of the most relevant dataset to identify fields related to premium pricing", "done_when": "Analyzed schema and identified fields related to premium pricing plans or tiers", "on_fail": {"action": "abort"} }, { "id": "s5", "description": "Retrieve sample queries to understand how premium plan data is typically queried", "done_when": "Retrieved and analyzed sample queries or determined no queries exist", "tool": "datahub__get_dataset_queries", "param_hints": { "urn": "<MOST_RELEVANT_URN_FROM_S3>", "source": "MANUAL", "count": 5 }, "on_fail": {"action": "continue"} }, { "id": "s6", "description": "Generate a summary of organizations on premium pricing plans based on findings", "done_when": "Created a comprehensive summary of organizations on premium plans or provided guidance on how to query this information", "on_fail": {"action": "abort"} } ], "expected_deliverable": "A list or description of organizations currently on premium pricing plans, including the data source used and how this information was determined. If direct list isn't available, provide guidance on how to query the relevant dataset(s) to obtain this information. For compliance purposes, only explicitly tagged entities count - no lineage inference." }

This plan will help you systematically discover and analyze premium pricing information."""

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = llm_response_with_trailing_text

            # With our fix, this should now work instead of raising JSONDecodeError
            plan = create_plan(
                session=mock_session,
                task="Find organizations on premium pricing plans",
            )

            # Verify the plan was created correctly
            assert plan.title == "Find Organizations on Premium Pricing Plans"
            assert "premium pricing plans" in plan.goal.lower()
            assert len(plan.steps) == 7  # s0 through s6
            assert plan.steps[0].id == "s0"
            assert plan.steps[6].id == "s6"
            assert "premium pricing" in plan.expected_deliverable.lower()

    def test_create_plan_generates_unique_ids(self, mock_session: ChatSession) -> None:
        """Test that multiple plans get unique IDs."""
        mock_response = """{
            "title": "Plan",
            "goal": "Goal",
            "assumptions": [],
            "steps": [{"id": "s0", "description": "Step", "done_when": "Done"}],
            "expected_deliverable": "Result"
        }"""

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = mock_response

            plan1 = create_plan(session=mock_session, task="Task 1")
            plan2 = create_plan(session=mock_session, task="Task 2")

            assert plan1.plan_id != plan2.plan_id
            assert plan1.plan_id in mock_session.plan_cache
            assert plan2.plan_id in mock_session.plan_cache


class TestRevisePlan:
    """Tests for revise_plan function."""

    @pytest.fixture
    def mock_session_with_plan(self) -> ChatSession:
        """Create a mock session with an existing plan."""
        session = MagicMock(spec=ChatSession)
        session.session_id = "test_session"

        # Create an initial plan
        plan = Plan(
            plan_id="plan_abc",
            version=1,
            title="Original Plan",
            goal="Find and analyze dataset",
            assumptions=["Dataset exists"],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=20),
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

        session.plan_cache = {
            "plan_abc": {
                "plan": plan,
                "progress": {
                    "s0": {"status": "completed", "evidence": {}, "confidence": 1.0}
                },
            }
        }
        session.get_plannable_tools = MagicMock(return_value=[])

        return session

    def test_revise_plan_success(self, mock_session_with_plan: ChatSession) -> None:
        """Test successful plan revision."""
        revision_response = """{
            "assumptions": ["Dataset exists", "Need broader search"],
            "steps": [
                {
                    "id": "s1",
                    "description": "Get downstream lineage with more hops",
                    "done_when": "Retrieved lineage with depth 3"
                },
                {
                    "id": "s2",
                    "description": "Filter for BI assets",
                    "done_when": "Found BI dashboards"
                }
            ],
            "expected_deliverable": "List of affected BI assets"
        }"""

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = revision_response

            revised_plan = revise_plan(
                session=mock_session_with_plan,
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

    def test_revise_plan_not_found(self, mock_session_with_plan: ChatSession) -> None:
        """Test revising a non-existent plan raises error."""
        with pytest.raises(ValueError, match="not found in session cache"):
            revise_plan(
                session=mock_session_with_plan,
                plan_id="nonexistent",
                completed_steps=[],
                current_step="s0",
                issue="Test issue",
            )

    def test_revise_plan_updates_cache(
        self, mock_session_with_plan: ChatSession
    ) -> None:
        """Test that revised plan updates the cache."""
        revision_response = """{
            "assumptions": ["Updated"],
            "steps": [{"id": "s1", "description": "New step", "done_when": "Done"}],
            "expected_deliverable": "Result"
        }"""

        with patch(
            "datahub_integrations.chat.planner.tools._call_planner_llm"
        ) as mock_llm:
            mock_llm.return_value = revision_response

            revised_plan = revise_plan(
                session=mock_session_with_plan,
                plan_id="plan_abc",
                completed_steps=["s0"],
                current_step="s1",
                issue="Issue",
            )

            # Verify cache is updated
            cached_plan = mock_session_with_plan.plan_cache["plan_abc"]["plan"]
            assert cached_plan.version == 2
            assert cached_plan == revised_plan


class TestReportStepProgress:
    """Tests for report_step_progress function."""

    @pytest.fixture
    def mock_session_with_plan(self) -> ChatSession:
        """Create a mock session with a plan."""
        session = MagicMock(spec=ChatSession)
        session.session_id = "test_session"

        plan = Plan(
            plan_id="plan_xyz",
            version=1,
            title="Test Plan",
            goal="Test Goal",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_tool_calls=10),
            steps=[
                Step(id="s0", description="Step 0", done_when="Done 0"),
                Step(id="s1", description="Step 1", done_when="Done 1"),
                Step(id="s2", description="Step 2", done_when="Done 2"),
            ],
            expected_deliverable="Result",
        )

        session.plan_cache = {"plan_xyz": {"plan": plan, "progress": {}}}

        return session

    def test_report_step_completed(self, mock_session_with_plan: ChatSession) -> None:
        """Test reporting a completed step."""
        message = report_step_progress(
            session=mock_session_with_plan,
            plan_id="plan_xyz",
            step_id="s0",
            status="completed",
            done_criteria_met=True,
            evidence={"result": "success"},
            confidence=1.0,
        )

        assert "s0 completed" in message
        assert "Next: s1" in message
        assert "1/3 complete" in message

        # Verify progress is stored
        progress = mock_session_with_plan.plan_cache["plan_xyz"]["progress"]
        assert "s0" in progress
        assert progress["s0"]["status"] == "completed"
        assert progress["s0"]["confidence"] == 1.0

    def test_report_step_failed(self, mock_session_with_plan: ChatSession) -> None:
        """Test reporting a failed step."""
        message = report_step_progress(
            session=mock_session_with_plan,
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
        self, mock_session_with_plan: ChatSession
    ) -> None:
        """Test reporting completion of the last step."""
        # Complete first two steps
        report_step_progress(
            session=mock_session_with_plan,
            plan_id="plan_xyz",
            step_id="s0",
            status="completed",
        )
        report_step_progress(
            session=mock_session_with_plan,
            plan_id="plan_xyz",
            step_id="s1",
            status="completed",
        )

        # Complete last step
        message = report_step_progress(
            session=mock_session_with_plan,
            plan_id="plan_xyz",
            step_id="s2",
            status="completed",
            done_criteria_met=True,
        )

        assert "Plan complete" in message
        assert "3 steps" in message

    def test_report_step_invalid_plan_id(
        self, mock_session_with_plan: ChatSession
    ) -> None:
        """Test reporting progress for non-existent plan raises error."""
        with pytest.raises(ValueError, match="not found in session cache"):
            report_step_progress(
                session=mock_session_with_plan,
                plan_id="nonexistent",
                step_id="s0",
                status="started",
            )

    def test_report_step_invalid_step_id(
        self, mock_session_with_plan: ChatSession
    ) -> None:
        """Test reporting progress for non-existent step raises error."""
        with pytest.raises(ValueError, match="not found in plan"):
            report_step_progress(
                session=mock_session_with_plan,
                plan_id="plan_xyz",
                step_id="s999",
                status="started",
            )

    def test_report_step_in_progress(self, mock_session_with_plan: ChatSession) -> None:
        """Test reporting a step in progress."""
        message = report_step_progress(
            session=mock_session_with_plan,
            plan_id="plan_xyz",
            step_id="s0",
            status="in_progress",
            evidence={"partial_results": []},
        )

        assert "Step recorded" in message
        assert "0/3 complete" in message

    def test_report_multiple_steps_tracks_progress(
        self, mock_session_with_plan: ChatSession
    ) -> None:
        """Test that reporting multiple steps correctly tracks progress."""
        # Complete s0
        msg1 = report_step_progress(
            session=mock_session_with_plan,
            plan_id="plan_xyz",
            step_id="s0",
            status="completed",
        )
        assert "1/3 complete" in msg1

        # Complete s1
        msg2 = report_step_progress(
            session=mock_session_with_plan,
            plan_id="plan_xyz",
            step_id="s1",
            status="completed",
        )
        assert "2/3 complete" in msg2

        # Verify progress cache has both
        progress = mock_session_with_plan.plan_cache["plan_xyz"]["progress"]
        assert len(progress) == 2
        assert progress["s0"]["status"] == "completed"
        assert progress["s1"]["status"] == "completed"
